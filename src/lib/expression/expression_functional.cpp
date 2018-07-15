#include "expression_functional.hpp"

namespace opossum {

namespace expression_functional {

std::shared_ptr<AbstractExpression> to_expression(const std::shared_ptr<AbstractExpression>& expression) {
  return expression;
}

std::shared_ptr<LQPColumnExpression> to_expression(const LQPColumnReference& column_reference) {
  return std::make_shared<LQPColumnExpression>(column_reference);
}

std::shared_ptr<ValueExpression> to_expression(const AllTypeVariant& value) {
  return std::make_shared<ValueExpression>(value);
}

std::shared_ptr<ValueExpression> value_(const AllTypeVariant& value) { return std::make_shared<ValueExpression>(value); }

std::shared_ptr<ValueExpression> null_() { return std::make_shared<ValueExpression>(NullValue{}); }

std::shared_ptr<ParameterExpression> parameter(const ParameterID parameter_id) {
  return std::make_shared<ParameterExpression>(parameter_id);
}

std::shared_ptr<LQPColumnExpression> column(const LQPColumnReference& column_reference) {
  return std::make_shared<LQPColumnExpression>(column_reference);
}

std::shared_ptr<AggregateExpression> count_star() {
  return std::make_shared<AggregateExpression>(AggregateFunction::Count);
}

std::shared_ptr<ExistsExpression> exists(const std::shared_ptr<AbstractExpression>& select_expression) {
  return std::make_shared<ExistsExpression>(select_expression);
}

}  // namespace expression_functional

}  // namespace opossum