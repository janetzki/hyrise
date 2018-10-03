#include "generate_column_statistics.hpp"

namespace opossum {

/**
 * Specialisation for strings since they don't have numerical_limits and that's what the unspecialised implementation
 * uses.
 */
template <>
std::shared_ptr<BaseColumnStatistics> generate_column_statistics<std::string>(const Table& table,
                                                                              const ColumnID column_id) {
  std::unordered_set<std::string_view> distinct_set;
  distinct_set.reserve(table.row_count());

  auto null_value_count = size_t{0};

  // The base storage of these views is in the distinct_set
  auto min = std::string_view{};
  auto max = std::string_view{};

  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto base_segment = table.get_chunk(chunk_id)->get_segment(column_id);

    resolve_segment_type<std::string>(*base_segment, [&](auto& segment) {
      auto iterable = create_iterable_from_segment<std::string>(segment);
      iterable.for_each([&](const auto& segment_value) {
        if (segment_value.is_null()) {
          ++null_value_count;
        } else {
          distinct_set.emplace(segment_value.value());

          // If the distinct_set.size() is 1, this is the first seen value and automatically both min and max
          if (distinct_set.size() == 1 || (segment_value.value() < min)) {
            min = segment_value.value();
          }
          if (distinct_set.size() == 1 || (segment_value.value() > max)) {
            max = segment_value.value();
          }
        }
      });
    });
  }

  const auto null_value_ratio =
      table.row_count() > 0 ? static_cast<float>(null_value_count) / static_cast<float>(table.row_count()) : 0.0f;
  const auto distinct_count = static_cast<float>(distinct_set.size());

  return std::make_shared<ColumnStatistics<std::string>>(null_value_ratio, distinct_count, std::string{min},
                                                         std::string{max});
}

}  // namespace opossum
