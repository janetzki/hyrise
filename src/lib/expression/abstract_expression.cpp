#include "abstract_expression.hpp"

#include <queue>

#include "boost/functional/hash.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractExpression::AbstractExpression(const ExpressionType type, const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
  type(type), arguments(arguments) {

}

bool AbstractExpression::requires_calculation() const {
  return !arguments.empty();
}

bool AbstractExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;
//  if (!deep_equals_expressions(arguments, expression.arguments)) return false;
  return _shallow_equals(expression);
}

size_t AbstractExpression::hash() const {
  auto hash = boost::hash_value(static_cast<ExpressionType>(type));
  for (const auto& argument : arguments) {
    boost::hash_combine(hash, argument->hash());
  }
  boost::hash_combine(hash, _on_hash());
  return hash;
}

bool AbstractExpression::_shallow_equals(const AbstractExpression& expression) const {
  return true;
}

size_t AbstractExpression::_on_hash() const {
  return 0;
}

}  // namespace opoosum