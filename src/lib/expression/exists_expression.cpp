#include "exists_expression.hpp"

#include "lqp_select_expression.hpp"

#include <sstream>

namespace opossum {

ExistsExpression::ExistsExpression(const std::shared_ptr<AbstractExpression>& select):
  AbstractExpression(ExpressionType::Exists, {select}) {
  Assert(select->type == ExpressionType::Select, "EXISTS needs SelectExpression as argument");
}

std::shared_ptr<AbstractExpression> ExistsExpression::select() const {
  Assert(arguments[0]->type == ExpressionType::Select, "Expected to contain SelectExpression");
  return arguments[0];
}

std::string ExistsExpression::as_column_name() const {
  std::stringstream stream;

  stream << "EXISTS(" << select()->as_column_name() << ")";

  return stream.str();
}

std::shared_ptr<AbstractExpression> ExistsExpression::deep_copy() const {
  return std::make_shared<ExistsExpression>(select()->deep_copy());
}

DataType ExistsExpression::data_type() const {
  return DataType::Int; // Bool, but we don't have that :(
}

bool ExistsExpression::is_nullable() const {
  return false;
}

}  // namespace opossum