#pragma once

#include <atomic>
#include <iostream>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "all_type_variant.hpp"
#include "cardinality.hpp"

namespace opossum {

class HorizontalStatisticsSlice;
class Table;

/**
 * Container for all cardinality estimation statistics gathered about a Table. Also used to represent the estimation of
 * a temporary Table during Optimization.
 *
 * The Table is partioned horizontally into slices and statistics are assigned to each slice independently. Each slice
 * might cover any number of rows/chunks and is not bound to the Chunks in the original Table.
 */
class TableCardinalityEstimationStatistics {
 public:
  /**
   * Generates histograms for all columns
   */
  static std::shared_ptr<TableCardinalityEstimationStatistics> from_table(const Table& table);

  explicit TableCardinalityEstimationStatistics(const std::vector<DataType>& column_data_types);

  /**
   * @return Accumulated row counts of all horizontal slices
   */
  Cardinality row_count() const;

  size_t column_count() const;

  const std::vector<DataType> column_data_types;

  std::vector<std::shared_ptr<HorizontalStatisticsSlice>> cardinality_estimation_slices;

  // A hopefully temporary means to represent the number of rows deleted from a Table by the Delete operator.
  std::atomic<size_t> approx_invalid_row_count{0};
};

std::ostream& operator<<(std::ostream& stream, const TableCardinalityEstimationStatistics& table_statistics);

}  // namespace opossum
