#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/subquery_to_join_rule.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SubqueryToJoinRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "a");
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    a_c = node_a->get_column("c");
    a_a_expression = to_expression(a_a);
    a_b_expression = to_expression(a_b);
    a_c_expression = to_expression(a_c);

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    node_c = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "c");
    c_a = node_c->get_column("a");
    c_b = node_c->get_column("b");

    node_d = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "d");
    d_a = node_d->get_column("a");
    d_b = node_d->get_column("b");
    d_c = node_d->get_column("c");

    node_e = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "e");
    e_a = node_e->get_column("a");
    e_b = node_e->get_column("b");
    e_c = node_e->get_column("c");

    _rule = std::make_shared<SubqueryToJoinRule>();
  }

  std::shared_ptr<SubqueryToJoinRule> _rule;

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d, node_e;
  LQPColumnReference a_a, a_b, a_c, b_a, b_b, c_a, c_b, d_a, d_b, d_c, e_a, e_b, e_c;
  std::shared_ptr<LQPColumnExpression> a_a_expression, a_b_expression, a_c_expression;
};

// HELPER FUNCTION TESTS

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageCountsNodesNotUsages) {
  const auto parameter1 = correlated_parameter_(ParameterID{0}, a_a);
  const auto parameter2 = correlated_parameter_(ParameterID{1}, a_b);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression},
                                                                                    {ParameterID{1}, a_b_expression}};

  // clang-format off
  const auto lqp =
      PredicateNode::make(equals_(b_a, parameter1),
                          PredicateNode::make(and_(equals_(b_b, parameter1), equals_(b_b, parameter2)),
                                              node_b));
  // clang-format on

  const auto result = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_EQ(result, std::pair(false, size_t{2}));
}

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageIgnoresUnrelatedParameters) {
  const auto unrelated_parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {};

  // Would return not optimizable for relevant parameter
  // clang-format off
  const auto lqp =
      ProjectionNode::make(expression_vector(add_(b_a, unrelated_parameter)),
                           node_a);
  // clang-format on

  const auto result = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_EQ(result, std::pair(false, size_t{0}));
}

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageFindsUsagesInSubqueries) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};
  const auto subquery_lqp = PredicateNode::make(equals_(parameter, b_a), node_b);

  // clang-format off
  const auto lqp =
      PredicateNode::make(exists_(lqp_subquery_(subquery_lqp)),
                          node_a);
  // clang-format on

  const auto result = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_EQ(result, std::pair(false, size_t{1}));
}

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageReportsUnoptimizableUsageInProjection) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
      ProjectionNode::make(expression_vector(add_(b_a, parameter)),
                           node_b);
  // clang-format on

  const auto& [not_optimizable, _] = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_TRUE(not_optimizable);
}

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageReportsUnoptimizableUsageInJoin) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
      JoinNode::make(JoinMode::Inner, expression_vector(equals_(b_a, c_a), equals_(b_a, parameter)),
                     node_b,
                     node_c);
  // clang-format on

  const auto& [not_optimizable, _] = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_TRUE(not_optimizable);
}

TEST_F(SubqueryToJoinRuleTest, AdaptAggregateNode) {
  const auto aggregate_node = AggregateNode::make(expression_vector(a_a), expression_vector());
  const auto& original_expressions = aggregate_node->column_expressions();

  // a_a is already group by expression, check it is not added again
  const auto adapted_aggregate_node = SubqueryToJoinRule::adapt_aggregate_node(aggregate_node, {a_a_expression});
  EXPECT_EQ(adapted_aggregate_node->column_expressions().size(), size_t{1});

  // a_b is an additional required group by expression, check it is added
  EXPECT_EQ(std::find(original_expressions.cbegin(), original_expressions.cend(), a_b_expression),
            original_expressions.cend());
  const auto adapted_aggregate_node_2 = SubqueryToJoinRule::adapt_aggregate_node(aggregate_node, {a_b_expression});
  const auto& expressions = adapted_aggregate_node_2->column_expressions();
  EXPECT_NE(std::find(expressions.cbegin(), expressions.cend(), a_b_expression), expressions.cend());
}

TEST_F(SubqueryToJoinRuleTest, AdaptAliasNode) {
  const auto alias_node =
      AliasNode::make(expression_vector(a_a, a_a, a_b), std::vector<std::string>{"a_a", "alias_a_a", "alias_a_b"});
  const auto& original_expressions = alias_node->column_expressions();

  // no added duplicates, preserve multiple names for same column,
  const auto adapted_alias_node = SubqueryToJoinRule::adapt_alias_node(alias_node, {a_a_expression});
  EXPECT_EQ(adapted_alias_node->column_expressions().size(), size_t{3});

  // no additional aliases
  const auto adapted_alias_node2 = SubqueryToJoinRule::adapt_alias_node(alias_node, {a_b_expression});
  EXPECT_EQ(adapted_alias_node2->column_expressions().size(), size_t{3});

  // add if necessary
  EXPECT_EQ(std::find(original_expressions.cbegin(), original_expressions.cend(), a_c_expression),
            original_expressions.cend());
  const auto adapted_alias_node3 = SubqueryToJoinRule::adapt_alias_node(alias_node, {a_c_expression});
  const auto& expressions = adapted_alias_node3->column_expressions();
  EXPECT_NE(std::find(expressions.cbegin(), expressions.cend(), a_c_expression), expressions.cend());
}

TEST_F(SubqueryToJoinRuleTest, AdaptProjectionNode) {
  const auto projection_node = ProjectionNode::make(expression_vector(a_a, a_a));
  const auto& original_expressions = projection_node->column_expressions();

  // no added duplicates, preserve original duplicates
  const auto adapted_projection_node = SubqueryToJoinRule::adapt_projection_node(projection_node, {a_a_expression});
  EXPECT_EQ(adapted_projection_node->column_expressions().size(), size_t{2});

  // add if necessary
  EXPECT_EQ(std::find(original_expressions.cbegin(), original_expressions.cend(), a_b_expression),
            original_expressions.cend());
  const auto adapted_projection_node2 = SubqueryToJoinRule::adapt_projection_node(projection_node, {a_b_expression});
  const auto& expressions = adapted_projection_node2->column_expressions();
  EXPECT_NE(std::find(expressions.cbegin(), expressions.cend(), a_b_expression), expressions.cend());
}

TEST_F(SubqueryToJoinRuleTest, TryToExtractJoinPredicate) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // unsupported predicate type: exists
  const auto predicate_node = PredicateNode::make(exists_(lqp_subquery_(node_a)));
  EXPECT_FALSE(SubqueryToJoinRule::try_to_extract_join_predicate(predicate_node, {}, true));

  // unsupported predicate type: in
  const auto predicate_node2 = PredicateNode::make(in_(parameter, list_(1)), node_b);
  EXPECT_FALSE(SubqueryToJoinRule::try_to_extract_join_predicate(predicate_node2, {}, true));

  // unsupported predicate type: between
  const auto predicate_node3 = PredicateNode::make(between_(parameter, b_b, value_(100)), node_b);
  EXPECT_FALSE(SubqueryToJoinRule::try_to_extract_join_predicate(predicate_node3, {}, true));

  // unsupported predicate type: like
  const auto predicate_node4 = PredicateNode::make(like_(parameter, "%test%"), node_b);
  EXPECT_FALSE(SubqueryToJoinRule::try_to_extract_join_predicate(predicate_node4, {}, true));

  // unsupported predicate type: is null
  const auto predicate_node5 = PredicateNode::make(is_null_(parameter), node_b);
  EXPECT_FALSE(SubqueryToJoinRule::try_to_extract_join_predicate(predicate_node5, {}, true));

  // Non-equals predicate below aggregate
  const auto predicate_node6 = PredicateNode::make(less_than_(b_b, parameter), node_b);
  EXPECT_FALSE(SubqueryToJoinRule::try_to_extract_join_predicate(predicate_node6, {}, true));

  // Other side is not a column expression

  // Correlated parameter is from outer subquery/a placeholder
}

TEST_F(SubqueryToJoinRuleTest, FindPullablePredicateNodesCanPullEqualsFromBelowAggregate) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  AggregateNode::make(expression_vector(), expression_vector(max_(b_a)),
    PredicateNode::make(equals_(b_a, parameter),
      node_b));
  // clang-format on

  const auto& predicate_node = lqp->left_input();

  const auto pullable_nodes = SubqueryToJoinRule::find_pullable_predicate_nodes(lqp, parameter_map);
  EXPECT_EQ(pullable_nodes.size(), 1);
  EXPECT_EQ(pullable_nodes.front().first, predicate_node);
}

TEST_F(SubqueryToJoinRuleTest, FindPullablePredicateNodesCannotPullNonEqualsFromBelowAggregate) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  AggregateNode::make(expression_vector(), expression_vector(max_(b_a)),
    PredicateNode::make(less_than_(b_a, parameter),
      node_b));
  // clang-format on

  const auto pullable_nodes = SubqueryToJoinRule::find_pullable_predicate_nodes(lqp, parameter_map);
  EXPECT_TRUE(pullable_nodes.empty());
}

TEST_F(SubqueryToJoinRuleTest, FindPullablePredicateNodesCanPullFromBothSidesOfInnerJoin) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
    PredicateNode::make(greater_than_(b_a, parameter),
      node_b),
    PredicateNode::make(equals_(c_a, parameter),
      node_c));
  // clang-format on

  const auto pullable_nodes = SubqueryToJoinRule::find_pullable_predicate_nodes(lqp, parameter_map);
  EXPECT_EQ(pullable_nodes.size(), 2);
}

TEST_F(SubqueryToJoinRuleTest, FindPullablePredicateNodesCanPullFromBothSidesOfCrossJoin) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
    PredicateNode::make(greater_than_(b_a, parameter),
      node_b),
    PredicateNode::make(equals_(c_a, parameter),
      node_c));
  // clang-format on

  const auto pullable_nodes = SubqueryToJoinRule::find_pullable_predicate_nodes(lqp, parameter_map);
  EXPECT_EQ(pullable_nodes.size(), 2);
}

TEST_F(SubqueryToJoinRuleTest, FindPullablePredicateNodesCanPullFromNonNullProducingSidesOfOuterJoins) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  const auto join_predicate = equals_(b_a, c_a);
  const auto left_predicate_node = PredicateNode::make(greater_than_(b_a, parameter), node_b);
  const auto right_predicate_node = PredicateNode::make(equals_(c_a, parameter), node_c);

  // clang-format off
  const auto full_outer_lqp =
  JoinNode::make(JoinMode::FullOuter, join_predicate,
    left_predicate_node,
    right_predicate_node);
  const auto left_outer_lqp =
  JoinNode::make(JoinMode::Left, join_predicate,
    left_predicate_node,
    right_predicate_node);
  const auto right_outer_lqp =
  JoinNode::make(JoinMode::Right, join_predicate,
    left_predicate_node,
    right_predicate_node);
  // clang-format on

  const auto full_outer_result = SubqueryToJoinRule::find_pullable_predicate_nodes(full_outer_lqp, parameter_map);
  EXPECT_TRUE(full_outer_result.empty());

  const auto left_outer_result = SubqueryToJoinRule::find_pullable_predicate_nodes(left_outer_lqp, parameter_map);
  EXPECT_EQ(left_outer_result.size(), 1);
  EXPECT_EQ(left_outer_result.front().first, left_predicate_node);

  const auto right_outer_result = SubqueryToJoinRule::find_pullable_predicate_nodes(right_outer_lqp, parameter_map);
  EXPECT_EQ(right_outer_result.size(), 1);
  EXPECT_EQ(right_outer_result.front().first, right_predicate_node);
}

TEST_F(SubqueryToJoinRuleTest, FindPullablePredicateNodesCanPullFromLeftSideOfSemiAntiJoins) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  for (const auto join_mode : {JoinMode::Semi, JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse}) {
    // clang-format off
    const auto lqp =
    JoinNode::make(join_mode, equals_(b_a, c_a),
      PredicateNode::make(not_equals_(b_a, parameter),
        node_b),
      PredicateNode::make(less_than_equals_(c_a, parameter),
        node_c));
    // clang-format on

    const auto result = SubqueryToJoinRule::find_pullable_predicate_nodes(lqp, parameter_map);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result.front().first, lqp->left_input());
  }
}

TEST_F(SubqueryToJoinRuleTest, FindPullablePredicateNodesCannotPullFromBelowLimits) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  LimitNode::make(value_(1),
    PredicateNode::make(equals_(b_a, parameter),
      node_b));
  // clang-format on

  const auto pullable_nodes = SubqueryToJoinRule::find_pullable_predicate_nodes(lqp, parameter_map);
  EXPECT_TRUE(pullable_nodes.empty());
}

TEST_F(SubqueryToJoinRuleTest, CopyAndAdaptLqpRemovesPullablePredicates) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

  // clang-format off
  const auto lqp =
  PredicateNode::make(equals_(b_a, parameter),
    node_b);
  // clang-format on

  const auto info = SubqueryToJoinRule::copy_and_adapt_lqp(lqp, {{lqp, equals_(b_a, a_a)}});
  EXPECT_LQP_EQ(info.adapted_lqp, node_b->deep_copy());
}

TEST_F(SubqueryToJoinRuleTest, CopyAndAdaptLqpDoesNotChangeOriginalLQPNodes) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

  // clang-format off
  const auto lqp =
  PredicateNode::make(equals_(b_a, b_b),
    PredicateNode::make(equals_(b_a, parameter),
      node_b));
  // clang-format on

  const auto lqp_copy = lqp->deep_copy();
  SubqueryToJoinRule::copy_and_adapt_lqp(lqp, {{lqp, equals_(b_a, a_a)}});
  EXPECT_LQP_EQ(lqp, lqp_copy);
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoHandlesCorrelatedExists) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const auto subquery_lqp = PredicateNode::make(equals_(b_a, parameter), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp, std::pair{ParameterID{0}, a_a});
  const auto lqp = PredicateNode::make(exists_(subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(input_info->subquery, subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::Semi);
  EXPECT_FALSE(input_info->join_predicate);
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoHandlesCorrelatedNotExists) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const auto subquery_lqp = PredicateNode::make(equals_(b_a, parameter), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp, std::pair{ParameterID{0}, a_a});
  const auto lqp = PredicateNode::make(not_exists_(subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(input_info->subquery, subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::AntiNullAsFalse);
  EXPECT_FALSE(input_info->join_predicate);
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoHandlesIn) {
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto lqp = PredicateNode::make(in_(a_a, subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(input_info->subquery, subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::Semi);
  EXPECT_EQ(*input_info->join_predicate, *equals_(a_a, b_a));
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoHandlesUncorrelatedNotIn) {
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto lqp = PredicateNode::make(not_in_(a_a, subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(input_info->subquery, subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::AntiNullAsTrue);
  EXPECT_EQ(*input_info->join_predicate, *equals_(a_a, b_a));
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoHandlesComparison) {
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto lqp = PredicateNode::make(less_than_(a_a, subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(input_info->subquery, subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::Semi);
  EXPECT_EQ(*input_info->join_predicate, *less_than_(a_a, b_a));
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoRejectsCorrelatedNotIn) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(equals_(b_a, parameter),
      node_b));
  // clang-format on

  const auto subquery_expression = lqp_subquery_(subquery_lqp, std::pair{ParameterID{0}, a_a});
  const auto lqp = PredicateNode::make(not_in_(a_a, subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(lqp);
  EXPECT_FALSE(input_info);
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoRejectsInWithConstantList) {
  // See #1546
  const auto lqp = PredicateNode::make(in_(a_a, list_(value_(1), value_(2))), node_a);
  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(lqp);
  EXPECT_FALSE(input_info);
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoRejectsUncorrelatedExists) {
  const auto subquery_expression = lqp_subquery_(node_b);
  for (const auto& predicate : {exists_(subquery_expression), not_exists_(subquery_expression)}) {
    const auto lqp = PredicateNode::make(predicate, node_a);
    const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(lqp);
    EXPECT_FALSE(input_info);
  }
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoLeftInOperandMustBeAColumnExpression) {
  // See #1547
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto predicate = in_(add_(a_a, 2), subquery_expression);

  const auto reject_lqp = PredicateNode::make(predicate, node_a);
  EXPECT_FALSE(SubqueryToJoinRule::is_predicate_node_join_candidate(reject_lqp));

  // clang-format off
  const auto accept_lqp =
  PredicateNode::make(predicate,
    ProjectionNode::make(expression_vector(add_(a_a, 2)),
      node_a));
  // clang-format on

  EXPECT_TRUE(SubqueryToJoinRule::is_predicate_node_join_candidate(accept_lqp));
}

TEST_F(SubqueryToJoinRuleTest, ExtractInputLQPInfoLeftComparisonOperandMustBeAColumnExpression) {
  // See #1547
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto predicate = less_than_(add_(a_a, 2), subquery_expression);

  const auto reject_lqp = PredicateNode::make(predicate, node_a);
  EXPECT_FALSE(SubqueryToJoinRule::is_predicate_node_join_candidate(reject_lqp));

  // clang-format off
  const auto accept_lqp =
  PredicateNode::make(predicate,
    ProjectionNode::make(expression_vector(add_(a_a, 2)),
      node_a));
  // clang-format on

  EXPECT_TRUE(SubqueryToJoinRule::is_predicate_node_join_candidate(accept_lqp));
}

// LQP INTEGRATION TESTS

TEST_F(SubqueryToJoinRuleTest, UncorrelatedInToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b)

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_a, b_a),
    node_a,
    ProjectionNode::make(expression_vector(b_a),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedInToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(equals_(b_b, parameter), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_a, b_a), equals_(a_b, b_b)),
    node_a,
    ProjectionNode::make(expression_vector(b_a, b_b),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedExistsToSemiJoin) {
  // SELECT * FROM a WHERE EXISTS (SELECT * FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  PredicateNode::make(equals_(b_b, parameter),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(exists_(subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_b, b_b),
    node_a,
    node_b);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedExistsWithAlias) {
  // SELECT * FROM d WHERE EXISTS (SELECT e.a AS b, e.b AS a FROM e WHERE e.b = d.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, d_b);

  // clang-format off
  const auto subquery_lqp =
  AliasNode::make(expression_vector(e_a, e_b), std::vector<std::string>({"b", "a"}),
    ProjectionNode::make(expression_vector(e_a),
      PredicateNode::make(equals_(e_b, parameter),
        node_e)));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b));

  const auto input_lqp =
  PredicateNode::make(exists_(subquery), node_d);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(d_b, e_b),
    node_d,
    AliasNode::make(expression_vector(e_a, e_b), std::vector<std::string>({"b", "a"}),
      ProjectionNode::make(expression_vector(e_a, e_b),
      node_e)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, DoubleCorrelatedExistsToSemiJoin) {
  // SELECT * FROM d WHERE EXISTS (SELECT * FROM e WHERE e.b = d.b AND e.c < d.c)

  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(e_a),
    PredicateNode::make(equals_(e_b, parameter0),
      PredicateNode::make(less_than_(e_c, parameter1),
        node_e)));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(exists_(subquery),
    node_d);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(d_b, e_b), greater_than_(d_c, e_c)),
    node_d,
    ProjectionNode::make(expression_vector(e_a, e_c, e_b),
      node_e));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedInWithAdditionToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a + 2 FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto b_a_plus_2 = add_(b_a, value_(2));
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a_plus_2),
    PredicateNode::make(equals_(b_b, parameter),
      node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_a, b_a_plus_2), equals_(a_b, b_b)),
    node_a,
    ProjectionNode::make(expression_vector(b_a_plus_2, b_b),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedNestedInToSemiJoins) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.b IN (SELECT c.a FROM c WHERE c.a < a.a))

  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

  // clang-format off
  const auto inner_subquery_lqp =
  ProjectionNode::make(expression_vector(c_a),
    PredicateNode::make(less_than_(c_a, parameter),
      node_c));

  const auto inner_subquery = lqp_subquery_(inner_subquery_lqp, std::make_pair(ParameterID{0}, a_a));

  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(in_(b_b, inner_subquery),
      node_b));

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_a, b_a),
    node_a,
    ProjectionNode::make(expression_vector(b_a),
      JoinNode::make(JoinMode::Semi, expression_vector(equals_(b_b, c_a), greater_than_(a_a, c_a)),
        node_b,
        ProjectionNode::make(expression_vector(c_a),
          node_c))));
  // TODO(janetzki): This does not seem to be the actual behavior.
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, UncorrelatedNestedInToSemiJoins) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.a IN (SELECT c.a FROM c))

  // clang-format off
  const auto inner_subquery_lqp =
  ProjectionNode::make(expression_vector(c_a),
    node_c);

  const auto inner_subquery = lqp_subquery_(inner_subquery_lqp);

  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(in_(b_a, inner_subquery),
      node_b));

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_a, b_a),
    node_a,
    ProjectionNode::make(expression_vector(b_a),
      JoinNode::make(JoinMode::Semi, equals_(b_a, c_a),
        node_b,
        ProjectionNode::make(expression_vector(c_a),
          node_c))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, UncorrelatedNotInToAntiJoin) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b)

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::AntiNullAsTrue, equals_(a_a, b_a),
    node_a,
    ProjectionNode::make(expression_vector(b_a),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, DoubleCorrelatedInToSemiJoin) {
  // SELECT * FROM d WHERE d.a IN (SELECT e.a FROM e WHERE e.b = d.b AND e.c < d.c)

  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(e_a),
    PredicateNode::make(equals_(e_b, parameter0),
      PredicateNode::make(less_than_(e_c, parameter1),
        node_e)));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(in_(d_a, subquery),
    node_d);

  const auto join_predicates = expression_vector(equals_(d_a, e_a), equals_(d_b, e_b), greater_than_(d_c, e_c));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, join_predicates,
    node_d,
    ProjectionNode::make(expression_vector(e_a, e_c, e_b),
      node_e));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, UncorrelatedComparatorToSemiJoin) {
  // SELECT * FROM a WHERE a.a = (SELECT SUM(b.a) FROM b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(equals_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_a, sum_(b_a))),
    node_a,
    AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedComparatorToSemiJoin) {
  // SELECT * FROM a WHERE a.a > (SELECT SUM(b.a) FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
    PredicateNode::make(equals_(b_b, parameter),
      node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_b, b_b), greater_than_(a_a, sum_(b_a))),
    node_a,
    AggregateNode::make(expression_vector(b_b), expression_vector(sum_(b_a)),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, DoubleCorrelatedComparatorToSemiJoin) {
  // SELECT * FROM a WHERE d.a > (SELECT SUM(e.a) FROM e WHERE e.b = d.b AND e.c = d.c)

  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(e_a)),
    PredicateNode::make(equals_(e_b, parameter0),
      PredicateNode::make(equals_(e_c, parameter1),
        node_e)));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(greater_than_(d_a, subquery),
    node_d);

  const auto join_predicates = expression_vector(equals_(d_b, e_b), greater_than_(d_a, sum_(e_a)), equals_(d_c, e_c));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, join_predicates,
    node_d,
    AggregateNode::make(expression_vector(e_c, e_b), expression_vector(sum_(e_a)),
      node_e));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// We expect to run after the PredicateSplitUpRule. Therefore, we do not handle multiple predicates joined by AND.
TEST_F(SubqueryToJoinRuleTest, NoRewriteOfAnd) {
  // SELECT * FROM d WHERE d.a IN (SELECT e.a FROM e WHERE e.b = d.b AND e.c < d.c)

  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(e_a),
    PredicateNode::make(and_(equals_(e_b, parameter0), less_than_(e_c, parameter1)),
      node_e));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(in_(d_a, subquery),
    node_d);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteConstantIn) {
  // SELECT * FROM a WHERE IN (1, 2, 3)

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(in_(a_a, list_(1, 2, 3)),
    node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteUncorrelatedExists) {
  // SELECT * FROM a WHERE (NOT) EXISTS (SELECT * FROM b)

  const auto subquery = lqp_subquery_(node_b);

  std::vector<std::shared_ptr<ExistsExpression>> predicates;
  predicates.emplace_back(exists_(subquery));
  predicates.emplace_back(not_exists_(subquery));

  for (const auto& predicate : predicates) {
    // clang-format off
    const auto input_lqp =
    PredicateNode::make(predicate,
      node_a);

    const auto expected_lqp = input_lqp->deep_copy();
    // clang-format on

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteCorrelatedNotIn) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(equals_(b_b, parameter),
      node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery),
    node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// The reformulation requires semi-/anti-join support in the SortMergeJoin operator (#1497).
TEST_F(SubqueryToJoinRuleTest, NoRewriteIfNoEqualsPredicateCanBeDerived) {
  // SELECT * FROM a WHERE EXISTS (SELECT * FROM b WHERE b.b < a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  PredicateNode::make(less_than_(b_b, parameter),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(exists_(subquery),
    node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum