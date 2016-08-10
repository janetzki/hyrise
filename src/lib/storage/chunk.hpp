#pragma once

#include "base_column.hpp"
#include "value_column.hpp"

namespace opossum {

class chunk {
public:
	chunk();
    chunk(std::vector<std::string>& column_types);
	chunk(const chunk&) = delete;
	chunk(chunk&&) = default;

	void add_column(std::string type);
	void add_column(std::shared_ptr<base_column> column);
	void append(std::initializer_list<all_type_variant> values) DEV_ONLY;
	std::shared_ptr<base_column> get_column(size_t column_id) const;
	std::vector<int> column_string_widths(int max = 0) const;
	void print(std::ostream &out = std::cout, const std::vector<int> &column_string_widths = std::vector<int>()) const;
	size_t size() const;

protected:
	std::vector<std::shared_ptr<base_column>> _columns;
};

}