#pragma once

#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

/* JitExpression represents a SQL expression - this includes arithmetic and logical expressions as well as comparisons.
 * Each JitExpression works on JitTupleValues and is structured as a binary tree. All leaves of that tree reference a tuple
 * value in the JitRuntimeContext and are of type JitExpressionType::Column - independent of whether these values actually
 * came from a column, are literal values or placeholders.
 * Each JitExpression can compute its value and stores it in its assigned result JitTupleValue. JitExpressions are also
 * able to compute the data type of the expression they represent.
 *
 * Using AbstractExpression as a base class for JitExpressions seems like a logical choice. However, AbstractExpression
 * adds a lot of bloat during code specialization. We thus decided against deriving from it here.
 */
class JitExpression {
 public:
  explicit JitExpression(const JitTupleValue& tuple_value);
  JitExpression(const std::shared_ptr<const JitExpression>& child, const JitExpressionType expression_type,
                const size_t result_tuple_index);
  JitExpression(const std::shared_ptr<const JitExpression>& left_child, const JitExpressionType expression_type,
                const std::shared_ptr<const JitExpression>& right_child, const size_t result_tuple_index);

  std::string to_string() const;

  JitExpressionType expression_type() const { return _expression_type; }
  std::shared_ptr<const JitExpression> left_child() const { return _left_child; }
  std::shared_ptr<const JitExpression> right_child() const { return _right_child; }
  const JitTupleValue& result() const { return _result_value; }

  /* Triggers the (recursive) computation of the value represented by this expression.
   * The result is not returned, but stored in the _result_value tuple value.
   * The compute() function MUST be called before the result value in the runtime tuple can safely be accessed through
   * the _result_value.
   * The _result_value itself, however, can safely be passed around before (e.g. by calling the result() function),
   * since it only abstractly represents the result slot in the runtime tuple.
   */
  void compute(JitRuntimeContext& context) const;

  template <typename T>
  Value<T> compute_and_get(JitRuntimeContext& context) const;

  void set_load_column(const size_t input_column_index) const {
#if JIT_LAZY_LOAD
    auto& non_const_load_column = const_cast<bool&>(_load_column);
    non_const_load_column = true;
    auto& non_const_input_column_index = const_cast<size_t&>(_input_column_index);
    non_const_input_column_index = input_column_index;
#endif
  }

  void set_expression_type(const JitExpressionType expression_type) const {
    const_cast<JitExpressionType&>(_expression_type) = expression_type;
  }

 private:
  std::pair<const DataType, const bool> _compute_result_type();

  const std::shared_ptr<const JitExpression> _left_child;
  const std::shared_ptr<const JitExpression> _right_child;
  const JitExpressionType _expression_type;
  const JitTupleValue _result_value;
#if JIT_LAZY_LOAD
  const bool _load_column = false;
  const size_t _input_column_index = 0;
#endif
};

}  // namespace opossum
