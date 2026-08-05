#include <Storages/StorageShardTelemetry.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatSettings.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

using URIParams = std::vector<std::pair<std::string, std::string>>;
using InputColumns = std::unordered_map<std::string, ColumnWithTypeAndName>;

struct TimestampBounds
{
    std::optional<UInt64> start;
    std::optional<UInt64> end;
    bool impossible = false;

    void addStart(UInt64 value)
    {
        start = std::max(start.value_or(value), value);
        checkRange();
    }

    void addEnd(UInt64 value)
    {
        end = std::min(end.value_or(value), value);
        checkRange();
    }

    void checkRange()
    {
        impossible |= start && end && *start >= *end;
    }
};

struct Pushdown
{
    TimestampBounds timestamps;
    URIParams equalities;
    bool fully_supported = true;
};

struct TimestampOrderLimit
{
    bool descending;
    UInt64 limit;
};

bool isLogsURI(const String & uri)
{
    for (const auto & [name, value] : Poco::URI(uri).getQueryParameters())
        if (name == "relation")
            return value == "logs";
    return true;
}

bool isSafeIndexedClickHouseToken(std::string_view token)
{
    return !token.empty() && std::ranges::all_of(token, [](unsigned char byte)
    {
        return (byte >= '0' && byte <= '9') || (byte >= 'A' && byte <= 'Z') || (byte >= 'a' && byte <= 'z');
    });
}

std::optional<TimestampOrderLimit> timestampOrderLimit(const SelectQueryInfo & query_info)
{
    if (query_info.query_tree)
    {
        const auto * query = query_info.query_tree->as<QueryNode>();
        if (!query || !query->hasLimit() || query->hasOffset() || query->hasLimitBy() || query->isLimitWithTies()
            || !query->hasOrderBy() || query->getOrderBy().getNodes().size() != 1)
            return std::nullopt;

        const auto * limit = query->getLimit()->as<ConstantNode>();
        const auto * sort = query->getOrderBy().getNodes().front()->as<SortNode>();
        if (!limit || !sort || sort->withFill() || sort->hasFillFrom() || sort->hasFillTo()
            || sort->hasFillStep() || sort->hasFillStaleness())
            return std::nullopt;

        const auto * column = sort->getExpression()->as<ColumnNode>();
        const Field limit_field = limit->getValue();
        if (!column || column->getColumnName() != "timestamp" || limit_field.getType() != Field::Types::UInt64)
            return std::nullopt;

        const UInt64 limit_value = limit_field.safeGet<UInt64>();
        if (limit_value == 0)
            return std::nullopt;
        return TimestampOrderLimit{sort->getSortDirection() == SortDirection::DESCENDING, limit_value};
    }

    if (!query_info.query)
        return std::nullopt;
    const auto * select = query_info.query->as<ASTSelectQuery>();
    if (!select || select->limitOffset() || select->limitBy() || select->limit_with_ties)
        return std::nullopt;
    const auto limit_ast = select->limitLength();
    const auto order_by = select->orderBy();
    const auto * limit = limit_ast ? limit_ast->as<ASTLiteral>() : nullptr;
    if (!limit || limit->value.getType() != Field::Types::UInt64 || !order_by || order_by->children.size() != 1)
        return std::nullopt;
    const auto * element = order_by->children.front()->as<ASTOrderByElement>();
    if (!element || element->with_fill || element->children.empty() || (element->direction != 1 && element->direction != -1))
        return std::nullopt;
    const auto * identifier = element->children.front()->as<ASTIdentifier>();
    if (!identifier || identifier->shortName() != "timestamp")
        return std::nullopt;
    const UInt64 limit_value = limit->value.safeGet<UInt64>();
    if (limit_value == 0)
        return std::nullopt;
    return TimestampOrderLimit{element->direction == -1, limit_value};
}

const ActionsDAG::Node * unwrapAlias(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        node = node->children.front();
    return node;
}

std::optional<String> constantString(const ActionsDAG::Node * node)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::COLUMN || !node->column || node->column->empty())
        return std::nullopt;

    Field value = (*node->column)[0];
    if (value.getType() != Field::Types::String)
        return std::nullopt;
    return value.safeGet<String>();
}

std::optional<String> inputName(const ActionsDAG::Node * node, const InputColumns & inputs)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::INPUT)
        return std::nullopt;

    if (auto mapped = inputs.find(node->result_name); mapped != inputs.end())
        return mapped->second.name;
    return node->result_name;
}

bool isMessageExpression(const ActionsDAG::Node * node, const InputColumns & inputs)
{
    node = unwrapAlias(node);
    if (!node)
        return false;
    if (inputName(node, inputs) == "message")
        return true;
    if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base
        || node->function_base->getName() != "ifNull" || node->children.size() != 2)
        return false;
    const auto fallback = constantString(node->children[1]);
    return fallback && fallback->empty() && isMessageExpression(node->children[0], inputs);
}

std::optional<String> constantString(const QueryTreeNodePtr & node)
{
    const auto * constant = node ? node->as<ConstantNode>() : nullptr;
    if (!constant)
        return std::nullopt;
    const Field value = constant->getValue();
    if (value.getType() != Field::Types::String)
        return std::nullopt;
    return value.safeGet<String>();
}

bool isMessageExpression(const QueryTreeNodePtr & node)
{
    if (!node)
        return false;
    if (const auto * column = node->as<ColumnNode>())
        return column->getColumnName() == "message";
    const auto * function = node->as<FunctionNode>();
    if (!function || function->getFunctionName() != "ifNull" || function->getArguments().getNodes().size() != 2)
        return false;
    const auto & arguments = function->getArguments().getNodes();
    const auto fallback = constantString(arguments[1]);
    return fallback && fallback->empty() && isMessageExpression(arguments[0]);
}

std::optional<std::pair<String, String>> mapElement(const QueryTreeNodePtr & node)
{
    const auto * function = node ? node->as<FunctionNode>() : nullptr;
    if (!function || function->getFunctionName() != "arrayElement" || function->getArguments().getNodes().size() != 2)
        return std::nullopt;
    const auto & arguments = function->getArguments().getNodes();
    const auto * column = arguments[0]->as<ColumnNode>();
    auto key = constantString(arguments[1]);
    if (!column || !key || key->empty())
        return std::nullopt;
    const String & map_name = column->getColumnName();
    if (map_name != "labels" && map_name != "metadata" && map_name != "attributes"
        && map_name != "resource_attributes" && map_name != "scope_attributes")
        return std::nullopt;
    return std::pair{map_name, std::move(*key)};
}

bool appendMapEquality(
    std::optional<std::pair<String, String>> element,
    std::optional<String> value,
    URIParams & equalities)
{
    /// ClickHouse returns the String default ("") for a missing Map key.
    /// ShardTelemetry's exact-field index distinguishes missing from stored empty,
    /// so empty equality must remain a residual predicate.
    if (!element || !value || value->empty())
        return false;

    String parameter;
    if (element->first == "labels")
        parameter = "label.";
    else if (element->first == "metadata")
        parameter = "metadata.";
    else if (element->first == "attributes")
        parameter = "attribute.";
    else if (element->first == "resource_attributes")
        parameter = "resource.";
    else if (element->first == "scope_attributes")
        parameter = "scope.";
    else
        return false;
    equalities.emplace_back(parameter + element->second, std::move(*value));
    return true;
}

bool collectQueryTreePushdown(const QueryTreeNodePtr & node, URIParams & equalities)
{
    const auto * function = node ? node->as<FunctionNode>() : nullptr;
    if (!function)
        return false;
    const auto & arguments = function->getArguments().getNodes();
    if (function->getFunctionName() == "and")
    {
        bool fully_supported = true;
        for (const auto & child : arguments)
            fully_supported &= collectQueryTreePushdown(child, equalities);
        return fully_supported;
    }
    if ((function->getFunctionName() == "hasToken" || function->getFunctionName() == "hasTokenCaseInsensitive")
        && arguments.size() == 2 && isMessageExpression(arguments[0]))
    {
        auto token = constantString(arguments[1]);
        if (token && isSafeIndexedClickHouseToken(*token))
        {
            equalities.emplace_back(
                function->getFunctionName() == "hasToken" ? "message_token" : "message_token_ci",
                std::move(*token));
            return true;
        }
    }
    if (function->getFunctionName() == "equals" && arguments.size() == 2)
    {
        auto element = mapElement(arguments[0]);
        auto value = constantString(arguments[1]);
        if (!element || !value)
        {
            element = mapElement(arguments[1]);
            value = constantString(arguments[0]);
        }
        return appendMapEquality(std::move(element), std::move(value), equalities);
    }
    return false;
}

bool hasFilterClause(const SelectQueryInfo & query_info)
{
    if (query_info.query_tree)
    {
        const auto * query = query_info.query_tree->as<QueryNode>();
        return !query || query->hasPrewhere() || query->hasWhere();
    }
    const auto * select = query_info.query ? query_info.query->as<ASTSelectQuery>() : nullptr;
    return !select || select->prewhere() || select->where();
}

std::optional<UInt64> dateTime64Nanos(const ActionsDAG::Node * node)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::COLUMN || !node->column || node->column->empty())
        return std::nullopt;

    const auto * type = typeid_cast<const DataTypeDateTime64 *>(node->result_type.get());
    if (!type)
        return std::nullopt;

    const Int64 raw = (*node->column)[0].safeGet<DateTime64>().getValue();
    if (raw < 0 || type->getScale() > 9)
        return std::nullopt;

    static constexpr std::array<UInt64, 10> powers_of_ten{
        1ULL,
        10ULL,
        100ULL,
        1'000ULL,
        10'000ULL,
        100'000ULL,
        1'000'000ULL,
        10'000'000ULL,
        100'000'000ULL,
        1'000'000'000ULL,
    };
    const UInt64 multiplier = powers_of_ten[9 - type->getScale()];
    const UInt64 value = static_cast<UInt64>(raw);
    if (value > std::numeric_limits<UInt64>::max() / multiplier)
        return std::nullopt;
    return value * multiplier;
}

std::optional<std::pair<String, String>> mapElement(
    const ActionsDAG::Node * node,
    const InputColumns & inputs)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base
        || node->function_base->getName() != "arrayElement" || node->children.size() != 2)
        return std::nullopt;

    auto map_name = inputName(node->children[0], inputs);
    auto key = constantString(node->children[1]);
    if (!map_name || !key || key->empty()
        || (*map_name != "labels" && *map_name != "metadata" && *map_name != "attributes"
            && *map_name != "resource_attributes" && *map_name != "scope_attributes"))
        return std::nullopt;
    return std::pair{std::move(*map_name), std::move(*key)};
}

std::optional<std::pair<String, String>> mapSubcolumn(
    const ActionsDAG::Node * node,
    const InputColumns & inputs)
{
    auto name = inputName(node, inputs);
    if (!name)
        return std::nullopt;

    String map_name;
    std::string_view serialized_key;
    static constexpr std::array<std::string_view, 5> maps{
        "labels", "metadata", "attributes", "resource_attributes", "scope_attributes"};
    for (const auto candidate : maps)
    {
        const String prefix = String(candidate) + ".key_";
        if (name->starts_with(prefix))
        {
            map_name = candidate;
            serialized_key = std::string_view(*name).substr(prefix.size());
            break;
        }
    }
    if (map_name.empty())
        return std::nullopt;

    if (serialized_key.empty())
        return std::nullopt;

    try
    {
        DataTypeString key_type;
        auto key_column = key_type.createColumn();
        ReadBufferFromString buffer(serialized_key);
        key_type.getDefaultSerialization()->deserializeWholeText(*key_column, buffer, FormatSettings{});
        if (key_column->size() != 1)
            return std::nullopt;
        Field key = (*key_column)[0];
        if (key.getType() != Field::Types::String || key.safeGet<String>().empty())
            return std::nullopt;
        return std::pair{std::move(map_name), key.safeGet<String>()};
    }
    catch (...)
    {
        /// An unfamiliar future subcolumn encoding only disables pushdown.
        return std::nullopt;
    }
}

std::optional<std::pair<String, String>> mapLookup(
    const ActionsDAG::Node * node,
    const InputColumns & inputs)
{
    if (auto element = mapElement(node, inputs))
        return element;
    return mapSubcolumn(node, inputs);
}

String reversedComparison(const String & function)
{
    if (function == "less")
        return "greater";
    if (function == "lessOrEquals")
        return "greaterOrEquals";
    if (function == "greater")
        return "less";
    if (function == "greaterOrEquals")
        return "lessOrEquals";
    return function;
}

bool addTimestampComparison(
    const String & function,
    const ActionsDAG::Node * left,
    const ActionsDAG::Node * right,
    const InputColumns & inputs,
    TimestampBounds & bounds)
{
    auto name = inputName(left, inputs);
    auto nanos = dateTime64Nanos(right);
    String normalized = function;
    if (!name || *name != "timestamp" || !nanos)
    {
        name = inputName(right, inputs);
        nanos = dateTime64Nanos(left);
        normalized = reversedComparison(function);
    }
    if (!name || *name != "timestamp" || !nanos)
        return false;

    if (normalized == "greaterOrEquals")
        bounds.addStart(*nanos);
    else if (normalized == "greater")
    {
        if (*nanos == std::numeric_limits<UInt64>::max())
            bounds.impossible = true;
        else
            bounds.addStart(*nanos + 1);
    }
    else if (normalized == "less")
        bounds.addEnd(*nanos);
    else if (normalized == "lessOrEquals")
    {
        if (*nanos != std::numeric_limits<UInt64>::max())
            bounds.addEnd(*nanos + 1);
    }
    else if (normalized == "equals")
    {
        bounds.addStart(*nanos);
        if (*nanos == std::numeric_limits<UInt64>::max())
            bounds.impossible = true;
        else
            bounds.addEnd(*nanos + 1);
    }
    else
        return false;
    return true;
}

bool addMapEquality(
    const ActionsDAG::Node * left,
    const ActionsDAG::Node * right,
    const InputColumns & inputs,
    URIParams & equalities)
{
    auto element = mapLookup(left, inputs);
    auto value = constantString(right);
    if (!element || !value)
    {
        element = mapLookup(right, inputs);
        value = constantString(left);
    }
    return appendMapEquality(std::move(element), std::move(value), equalities);
}

bool addScalarEquality(
    const ActionsDAG::Node * left,
    const ActionsDAG::Node * right,
    const InputColumns & inputs,
    URIParams & equalities)
{
    auto name = inputName(left, inputs);
    auto value = constantString(right);
    if (!name || !value)
    {
        name = inputName(right, inputs);
        value = constantString(left);
    }
    if (!name || !value
        || (*name != "trace_id" && *name != "span_id" && *name != "series_id" && *name != "name"))
        return false;
    equalities.emplace_back(*name, std::move(*value));
    return true;
}

void collectPushdown(const ActionsDAG::Node * node, const InputColumns & inputs, Pushdown & pushdown)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
    {
        pushdown.fully_supported = false;
        return;
    }

    const String function = node->function_base->getName();
    if (function == "and")
    {
        for (const auto * child : node->children)
            collectPushdown(child, inputs, pushdown);
        return;
    }

    if (node->children.size() == 2
        && addTimestampComparison(function, node->children[0], node->children[1], inputs, pushdown.timestamps))
        return;

    if (function == "equals" && node->children.size() == 2
        && addMapEquality(node->children[0], node->children[1], inputs, pushdown.equalities))
        return;

    if (function == "equals" && node->children.size() == 2
        && addScalarEquality(node->children[0], node->children[1], inputs, pushdown.equalities))
        return;

    if ((function == "hasToken" || function == "hasTokenCaseInsensitive") && node->children.size() == 2
        && isMessageExpression(node->children[0], inputs))
    {
        auto token = constantString(node->children[1]);
        if (token && isSafeIndexedClickHouseToken(*token))
        {
            pushdown.equalities.emplace_back(
                function == "hasToken" ? "message_token" : "message_token_ci",
                std::move(*token));
            return;
        }
    }

    /// OR, NOT, LIKE, regexes, message functions, casts, and expressions over
    /// dynamic values stay in ClickHouse. Never guess at semantic equivalence.
    pushdown.fully_supported = false;
}

bool isShardTelemetryColumn(const String & name)
{
    static const std::unordered_set<String> columns{
        "tenant", "signal", "timestamp", "parent_timestamp", "observed_timestamp",
        "start_timestamp", "end_timestamp", "partition", "offset", "ordinal",
        "resource_id", "scope_id", "trace_id", "span_id", "parent_span_id",
        "linked_trace_id", "linked_span_id", "series_id", "message", "body_json",
        "name", "event_name", "severity_number", "severity_text", "kind", "duration_nanos",
        "status_code", "status_message", "trace_state", "flags", "dropped_attributes_count",
        "dropped_events_count", "dropped_links_count", "labels", "metadata", "attributes",
        "resource_attributes", "scope_attributes", "attribute_ids", "resource_attribute_ids",
        "scope_attribute_ids", "attributes_json", "resource_attributes_json",
        "scope_attributes_json", "events_json", "links_json", "description", "unit",
        "metric_kind", "temporality", "monotonic", "value_type", "scalar_integer",
        "scalar_double_bits", "value_json", "exemplars_json"};
    return columns.contains(name);
}

std::optional<String> physicalShardTelemetryColumn(const String & name)
{
    if (isShardTelemetryColumn(name))
        return name;
    for (const auto map : {"labels", "metadata", "attributes", "resource_attributes", "scope_attributes",
                           "attribute_ids", "resource_attribute_ids", "scope_attribute_ids"})
    {
        const String prefix = String(map) + ".";
        if (name.starts_with(prefix))
            return map;
    }
    return std::nullopt;
}

std::optional<String> projection(const Names & column_names)
{
    if (column_names.empty())
        /// ClickHouse requests no physical columns for count(). The Arrow
        /// endpoint still needs one lane to communicate block cardinality;
        /// offset is fixed-width, non-null, and cheap to encode.
        return "offset";

    std::unordered_set<String> seen;
    String result;
    for (const auto & name : column_names)
    {
        auto physical_name = physicalShardTelemetryColumn(name);
        if (!physical_name)
            return std::nullopt;
        if (!seen.emplace(*physical_name).second)
            continue;
        if (!result.empty())
            result += ',';
        result += *physical_name;
    }
    if (result.empty())
        return std::nullopt;
    return result;
}

}

std::vector<std::pair<std::string, std::string>> StorageShardTelemetry::getReadURIParams(
    const Names & column_names,
    const StorageSnapshotPtr &,
    const SelectQueryInfo & query_info,
    const ContextPtr &,
    QueryProcessingStage::Enum &,
    size_t) const
{
    URIParams params;
    /// StorageURL marks plain count()/count(*) reads explicitly after proving
    /// that the query has no WHERE, PREWHERE, row policy, grouping, or other
    /// aggregate. It still supplies an arbitrary smallest physical column, so
    /// column_names.empty() alone cannot recognize this path. Force the
    /// adapter's fixed UInt64 cardinality lane regardless of that choice.
    if (query_info.optimize_trivial_count || column_names.empty())
    {
        params.emplace_back("columns", "offset");
        params.emplace_back("cardinality_only", "1");
    }
    else if (auto columns = projection(column_names))
        params.emplace_back("columns", std::move(*columns));

    Pushdown pushdown;
    const bool has_filter = hasFilterClause(query_info);
    const bool has_filter_dag = query_info.filter_actions_dag && !query_info.filter_actions_dag->getOutputs().empty();
    bool filters_fully_supported = !has_filter;
    if (has_filter_dag)
    {
        const auto inputs = query_info.buildNodeNameToInputNodeColumn();
        collectPushdown(query_info.filter_actions_dag->getOutputs().front(), inputs, pushdown);
        filters_fully_supported = pushdown.fully_supported;
    }
    else if (has_filter && query_info.query_tree)
    {
        const auto * query = query_info.query_tree->as<QueryNode>();
        if (query && query->hasWhere() && !query->hasPrewhere())
            filters_fully_supported = collectQueryTreePushdown(query->getWhere(), pushdown.equalities);
    }

    const auto ordered_limit = isLogsURI(uri) ? timestampOrderLimit(query_info) : std::nullopt;
    if (pushdown.timestamps.impossible)
        params.emplace_back("limit", "0");
    else
    {
        if (pushdown.timestamps.start)
            params.emplace_back("start_ns", std::to_string(*pushdown.timestamps.start));
        if (pushdown.timestamps.end)
            params.emplace_back("end_ns", std::to_string(*pushdown.timestamps.end));
        params.insert(params.end(), pushdown.equalities.begin(), pushdown.equalities.end());
        if (ordered_limit && filters_fully_supported)
        {
            params.emplace_back("order", ordered_limit->descending ? "timestamp_desc" : "timestamp_asc");
            params.emplace_back("limit", std::to_string(ordered_limit->limit));
        }
        else if (query_info.trivial_limit && filters_fully_supported)
            params.emplace_back("limit", std::to_string(query_info.trivial_limit));
    }
    return params;
}

void registerStorageShardTelemetry(StorageFactory & factory)
{
    factory.registerStorage(
        "ShardTelemetry",
        [](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;
            auto configuration = StorageURL::getConfiguration(engine_args, args.getLocalContext(), &args.table_id);
            if (configuration.format != "ArrowStream")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "ShardTelemetry storage requires the ArrowStream format");

            auto context = args.getLocalContext();
            return std::make_shared<StorageShardTelemetry>(
                configuration.url,
                args.table_id,
                configuration.format,
                StorageURL::getFormatSettingsFromArgs(args),
                args.columns,
                args.constraints,
                args.comment,
                context,
                configuration.compression_method,
                configuration.headers,
                configuration.http_method);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = false,
            .source_access_type = AccessTypeObjects::Source::URL,
            .has_builtin_setting_fn = Settings::hasBuiltin,
        });
}

}
