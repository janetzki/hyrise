#include <thread>
#include <atomic>
#include <iostream>
#include <iostream>

#include "types.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "operators/print.hpp"
#include "sql/sql_pipeline_builder.hpp"

using namespace opossum;  // NOLINT
using namespace std::string_literals;  // NOLINT

int main() {
  const auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int}}, TableType::Data, 5, UseMvcc::Yes);
  //table->add_unique_constraint({ColumnID{0}}, false);

  StorageManager::get().add_table("T0", table);

  std::atomic<int32_t> global_variable{0};

  std::vector<std::thread> threads;

  for (size_t t{0}; t < 2; ++t) {
    threads.emplace_back([&global_variable, t]() {
      while (global_variable.load() < 1000) {
        const auto query = "INSERT INTO T0 VALUES("s + std::to_string(global_variable.load()) + ")";

        auto statement = SQLPipelineBuilder{query}.dont_cleanup_temporaries().create_pipeline_statement();
        statement.get_result_table();

        if (t == 0) global_variable++;
      }
    });
  }

  for (auto& thread : threads) thread.join();

  Print::print(table);

  return 0;
}
