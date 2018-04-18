#include "like_table_scan_impl.hpp"

#include <boost/algorithm/string/replace.hpp>

#include <algorithm>
#include <array>
#include <map>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "storage/column_iterables/constant_value_iterable.hpp"
#include "storage/column_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/resolve_encoded_column_type.hpp"
#include "storage/value_column.hpp"
#include "storage/value_column/value_column_iterable.hpp"

namespace opossum {

LikeTableScanImpl::LikeTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                     const PredicateCondition predicate_condition, const std::string& pattern, const TableScan& table_scan)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, predicate_condition, table_scan},
      _pattern{pattern},
      _invert_results(predicate_condition == PredicateCondition::NotLike) {
 _pattern_variant = pattern_string_to_pattern_variant(pattern);
}

void LikeTableScanImpl::handle_column(const BaseValueColumn& base_column,
                                      std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;
  auto& left_column = static_cast<const ValueColumn<std::string>&>(base_column);
  auto left_iterable = ValueColumnIterable<std::string>{left_column};

  _scan_iterable(left_iterable, chunk_id, matches_out, mapped_chunk_offsets.get());
}

void LikeTableScanImpl::handle_column(const BaseEncodedColumn& base_column,
                                      std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  resolve_encoded_column_type<std::string>(base_column, [&](const auto& typed_column) {
    auto left_iterable = create_iterable_from_column(typed_column);
    _scan_iterable(left_iterable, chunk_id, matches_out, mapped_chunk_offsets.get());
  });
}

void LikeTableScanImpl::handle_column(const BaseDictionaryColumn& base_column,
                                      std::shared_ptr<ColumnVisitableContext> base_context) {
  const auto& left_column = static_cast<const DictionaryColumn<std::string>&>(base_column);
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  const auto result = _find_matches_in_dictionary(*left_column.dictionary());
  const auto& match_count = result.first;
  const auto& dictionary_matches = result.second;

  auto attribute_vector_iterable = create_iterable_from_attribute_vector(left_column);

  // LIKE matches all rows
  if (match_count == dictionary_matches.size()) {
    attribute_vector_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      static const auto always_true = [](const auto&) { return true; };
      this->_unary_scan(always_true, left_it, left_end, chunk_id, matches_out);
    });

    return;
  }

  // LIKE matches no rows
  if (match_count == 0u) {
    return;
  }

  const auto dictionary_lookup = [&dictionary_matches](const ValueID& value) { return dictionary_matches[value]; };

  attribute_vector_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    this->_unary_scan(dictionary_lookup, left_it, left_end, chunk_id, matches_out);
  });
}

LikeTableScanImpl::PatternTokens LikeTableScanImpl::pattern_string_to_tokens(const std::string &pattern) {
  PatternTokens tokens;

  auto current_position = size_t{0};
  while (current_position < pattern.size()) {
    if (pattern[current_position] == '_') {
      tokens.emplace_back(PatternWildcard::SingleChar);
      ++current_position;
    } else if (pattern[current_position] == '%') {
      tokens.emplace_back(PatternWildcard::AnyChars);
      ++current_position;
    } else {
      const auto next_wildcard_position = pattern.find_first_of("_%", current_position);
      const auto token_length = next_wildcard_position == std::string::npos ? std::string::npos : next_wildcard_position - current_position;
      tokens.emplace_back(pattern.substr(current_position, token_length));
      current_position = next_wildcard_position;
    }
  }

  return tokens;
}

LikeTableScanImpl::PatternVariant LikeTableScanImpl::pattern_string_to_pattern_variant(const std::string &pattern) {
  const auto tokens = pattern_string_to_tokens(pattern);

  if (tokens.size() == 2 &&
      tokens[0].type() == typeid(std::string) &&
      tokens[1] == PatternToken{PatternWildcard::AnyChars}) {
    return StartsWithPattern{boost::get<std::string>(tokens[0])};
  } else if (tokens.size() == 2 &&
             tokens[0] == PatternToken{PatternWildcard::AnyChars} &&
             tokens[1].type() == typeid(std::string)) {
    return EndsWithPattern{boost::get<std::string>(tokens[1])};
  } else if (tokens.size() == 3 &&
             tokens[0] == PatternToken{PatternWildcard::AnyChars} &&
             tokens[1].type() == typeid(std::string) &&
             tokens[2] ==PatternToken{PatternWildcard::AnyChars}) {
    return ContainsPattern{boost::get<std::string>(tokens[1])};
  } else {
    // Pick ContainsMultiple or Regex
    auto impl_is_contains_multiple = true; // Set to false if tokens don't match %{, str, %} pattern
    auto strings = std::vector<std::string>{}; // arguments used for ContainsMultiple, if it gets used
    auto expect_any_chars = true;  // If false, expect a string

    for (const auto& token : tokens) {
      if (expect_any_chars && token != PatternToken{PatternWildcard::AnyChars}) {
        impl_is_contains_multiple = false;
        break;
      }
      if (!expect_any_chars && token.type() != typeid(std::string)) {
        impl_is_contains_multiple = false;
        break;
      }
      if (!expect_any_chars) {
        strings.emplace_back(boost::get<std::string>(token));
      }

      expect_any_chars = !expect_any_chars;
    }

    if (impl_is_contains_multiple) {
      return ContainsMultiplePattern{strings};
    } else {
      return std::regex(sql_like_to_regex(pattern));
    }
  }
}

template<typename Functor>
void LikeTableScanImpl::resolve_pattern_matcher(const PatternVariant &pattern_variant, const bool invert, const Functor &functor) {
  if (pattern_variant.type() == typeid(StartsWithPattern)) {
    const auto& prefix = boost::get<StartsWithPattern>(pattern_variant).str;
    functor([&](const std::string& str) -> bool {
      if (str.size() < prefix.size()) return invert;
      return (str.compare(0, prefix.size(), prefix) == 0) ^ invert;
    });

  } else if (pattern_variant.type() == typeid(EndsWithPattern)) {
    const auto& suffix = boost::get<EndsWithPattern>(pattern_variant).str;
    functor([&](const std::string& str) -> bool {
      if (str.size() < suffix.size()) return invert;
      return (str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0) ^ invert;
    });

  } else if (pattern_variant.type() == typeid(ContainsPattern)) {
    const auto& contains_str = boost::get<ContainsPattern>(pattern_variant).str;
    functor([&](const std::string& str) -> bool {
      return (str.find(contains_str) != std::string::npos) ^ invert;
    });

  } else if (pattern_variant.type() == typeid(ContainsMultiplePattern)) {
    const auto& contains_strs = boost::get<ContainsMultiplePattern>(pattern_variant).str;

    functor([&](const std::string& str) -> bool {
      auto current_position = size_t{0};
      for (const auto& contains_str : contains_strs) {
        current_position = str.find(contains_str, current_position);
        if (current_position == std::string::npos) return invert;
      }
      return !invert;
    });

  } else if (pattern_variant.type() == typeid(std::regex)) {
    const auto& regex = boost::get<std::regex>(pattern_variant);

    functor([&](const std::string& str) -> bool {
      return std::regex_match(str, regex) ^ invert;
    });

  } else {
    Fail("Impl not... implemented [sic]");
  }
}

std::string LikeTableScanImpl::sql_like_to_regex(std::string sql_like) {
  // Do substitution of <backslash> with <backslash><backslash> FIRST, because otherwise it will also replace
  // backslashes introduced by the other substitutions
  constexpr auto replace_by = std::array<std::pair<const char*, const char*>, 15u>{{{"\\", "\\\\"},
                                                                                   {".", "\\."},
                                                                                   {"^", "\\^"},
                                                                                   {"$", "\\$"},
                                                                                   {"+", "\\+"},
                                                                                   {"?", "\\?"},
                                                                                   {"(", "\\("},
                                                                                   {")", "\\)"},
                                                                                   {"{", "\\{"},
                                                                                   {"}", "\\}"},
                                                                                   {"|", "\\|"},
                                                                                   {".", "\\."},
                                                                                   {"*", "\\*"},
                                                                                   {"%", ".*"},
                                                                                   {"_", "."}}};

  for (const auto& pair : replace_by) {
    boost::replace_all(sql_like, pair.first, pair.second);
  }

  return "^" + sql_like + "$";
}

template<typename Iterable>
void LikeTableScanImpl::_scan_iterable(const Iterable& iterable,
                                       const ChunkID chunk_id,
                                       PosList& matches_out,
                                       const ChunkOffsetsList* const mapped_chunk_offsets) {
  this->resolve_pattern_matcher(_pattern_variant, _invert_results, [&](const auto &matcher) {
    iterable.with_iterators(mapped_chunk_offsets, [&](auto left_it, auto left_end) {
      this->_unary_scan(matcher, left_it, left_end, chunk_id, matches_out);
    });
  });
}


std::pair<size_t, std::vector<bool>> LikeTableScanImpl::_find_matches_in_dictionary(
const pmr_vector<std::string>& dictionary) {
  auto result = std::pair<size_t, std::vector<bool>>{};

  auto& count = result.first;
  auto& dictionary_matches = result.second;

  count = 0u;
  dictionary_matches.reserve(dictionary.size());

  resolve_pattern_matcher(_pattern_variant, _invert_results, [&](const auto &matcher) {
    for (const auto &value : dictionary) {
      const auto result = matcher(value);
      count += static_cast<size_t>(result);
      dictionary_matches.push_back(result);
    }
  });

  return result;
}

std::ostream& operator<<(std::ostream& stream, const LikeTableScanImpl::PatternWildcard& wildcard) {
  switch(wildcard) {
    case LikeTableScanImpl::PatternWildcard::SingleChar: stream << "_"; break;
    case LikeTableScanImpl::PatternWildcard::AnyChars: stream << "%"; break;
  }

  return stream;
}

}  // namespace opossum
