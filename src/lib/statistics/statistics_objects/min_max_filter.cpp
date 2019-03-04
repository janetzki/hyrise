#include "min_max_filter.hpp"

#include <memory>
#include <optional>
#include <utility>

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "statistics/abstract_statistics_object.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
MinMaxFilter<T>::MinMaxFilter(T min, T max) : AbstractStatisticsObject(data_type_from_type<T>()), min(min), max(max) {}

template <typename T>
CardinalityEstimate MinMaxFilter<T>::estimate_cardinality(const PredicateCondition predicate_type,
                                                          const AllTypeVariant& variant_value,
                                                          const std::optional<AllTypeVariant>& variant_value2) const {
  if (does_not_contain(predicate_type, variant_value, variant_value2)) {
    return {Cardinality{0}, EstimateType::MatchesNone};
  } else {
    return {Cardinality{0}, EstimateType::MatchesApproximately};
  }
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> MinMaxFilter<T>::sliced(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (does_not_contain(predicate_type, variant_value, variant_value2)) {
    return nullptr;
  }

  T sliced_min, sliced_max;
  const auto value = type_cast_variant<T>(variant_value);

  // If value is either sliced_min or max, we do not take the opportunity to slightly improve the new object.
  // We do not know the actual previous/next value, and for strings it's not that simple.
  // The impact should be small.
  switch (predicate_type) {
    case PredicateCondition::Equals:
      sliced_min = value;
      sliced_max = value;
      break;
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      sliced_min = min;
      sliced_max = value;
      break;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      sliced_min = value;
      sliced_max = max;
      break;
    case PredicateCondition::Between: {
      DebugAssert(variant_value2, "BETWEEN needs a second value.");
      const auto value2 = type_cast_variant<T>(*variant_value2);
      return sliced(PredicateCondition::GreaterThanEquals, value)->sliced(PredicateCondition::LessThanEquals, value2);
    }
    default:
      sliced_min = min;
      sliced_max = max;
  }

  const auto filter = std::make_shared<MinMaxFilter<T>>(sliced_min, sliced_max);
  return filter;
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> MinMaxFilter<T>::scaled(const float /*selectivity*/) const {
  return std::make_shared<MinMaxFilter<T>>(min, max);
}

template <typename T>
bool MinMaxFilter<T>::does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                       const std::optional<AllTypeVariant>& variant_value2) const {
  // Early exit for NULL variants.
  if (variant_is_null(variant_value)) {
    return false;
  }

  const auto value = type_cast_variant<T>(variant_value);

  // Operators work as follows: value_from_table <operator> value
  // e.g. OpGreaterThan: value_from_table > value
  // thus we can exclude chunk if value >= max since then no value from the table can be greater than value
  switch (predicate_type) {
    case PredicateCondition::GreaterThan:
      return value >= max;
    case PredicateCondition::GreaterThanEquals:
      return value > max;
    case PredicateCondition::LessThan:
      return value <= min;
    case PredicateCondition::LessThanEquals:
      return value < min;
    case PredicateCondition::Equals:
      return value < min || value > max;
    case PredicateCondition::NotEquals:
      return value == min && value == max;
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
      const auto value2 = type_cast_variant<T>(*variant_value2);
      return value > max || value2 < min;
    }
    default:
      return false;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(MinMaxFilter);

}  // namespace opossum