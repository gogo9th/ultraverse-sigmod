#include "StateItem.h"

#include <cctype>
#include <cstdlib>
#include <execution>
#include <iomanip>
#include <limits>
#include <sstream>

#include <boost/tuple/tuple.hpp>

#include "ultraverse_state.pb.h"

namespace {

struct DecimalParts {
  bool negative = false;
  std::string intPart;
  std::string fracPart;
};

std::string toHexLiteral(const std::string &input) {
  static const char *hex = "0123456789ABCDEF";
  std::string out;
  out.reserve(2 + input.size() * 2 + 1);
  out.push_back('X');
  out.push_back('\'');
  for (unsigned char ch : input) {
    out.push_back(hex[(ch >> 4) & 0x0F]);
    out.push_back(hex[ch & 0x0F]);
  }
  out.push_back('\'');
  return out;
}

std::string formatStateDataForWhere(const StateData &data) {
  switch (data.Type()) {
    case en_column_data_int: {
      int64_t value = 0;
      if (data.Get(value)) {
        return std::to_string(value);
      }
      return "NULL";
    }
    case en_column_data_uint: {
      uint64_t value = 0;
      if (data.Get(value)) {
        return std::to_string(value);
      }
      return "NULL";
    }
    case en_column_data_double: {
      double value = 0.0;
      if (data.Get(value)) {
        std::ostringstream out;
        out << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
        return out.str();
      }
      return "NULL";
    }
    case en_column_data_decimal: {
      std::string value;
      if (data.Get(value)) {
        return toHexLiteral(value);
      }
      return "NULL";
    }
    case en_column_data_string: {
      std::string value;
      if (data.Get(value)) {
        return toHexLiteral(value);
      }
      return "NULL";
    }
    default:
      return "NULL";
  }
}

bool parseDecimalString(const std::string &input, DecimalParts *out) {
  if (out == nullptr) {
    return false;
  }

  size_t start = 0;
  size_t end = input.size();
  while (start < end && std::isspace(static_cast<unsigned char>(input[start])) != 0) {
    start++;
  }
  while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1])) != 0) {
    end--;
  }

  if (start >= end) {
    return false;
  }

  bool negative = false;
  if (input[start] == '+' || input[start] == '-') {
    negative = (input[start] == '-');
    start++;
  }

  if (start >= end) {
    return false;
  }

  bool seenDot = false;
  std::string intPart;
  std::string fracPart;

  for (size_t i = start; i < end; ++i) {
    char c = input[i];
    if (c == '.') {
      if (seenDot) {
        return false;
      }
      seenDot = true;
      continue;
    }
    if (std::isdigit(static_cast<unsigned char>(c)) == 0) {
      return false;
    }
    if (seenDot) {
      fracPart.push_back(c);
    } else {
      intPart.push_back(c);
    }
  }

  if (intPart.empty() && fracPart.empty()) {
    return false;
  }

  if (intPart.empty()) {
    intPart = "0";
  }

  size_t firstNonZero = intPart.find_first_not_of('0');
  if (firstNonZero == std::string::npos) {
    intPart = "0";
  } else if (firstNonZero > 0) {
    intPart = intPart.substr(firstNonZero);
  }

  while (!fracPart.empty() && fracPart.back() == '0') {
    fracPart.pop_back();
  }

  if (intPart == "0" && fracPart.empty()) {
    negative = false;
  }

  out->negative = negative;
  out->intPart = std::move(intPart);
  out->fracPart = std::move(fracPart);
  return true;
}

std::string normalizeDecimalString(const std::string &input, bool *ok) {
  DecimalParts parts;
  bool parsed = parseDecimalString(input, &parts);
  if (ok != nullptr) {
    *ok = parsed;
  }
  if (!parsed) {
    return input;
  }

  std::string normalized;
  if (parts.negative) {
    normalized.push_back('-');
  }
  normalized += parts.intPart;
  if (!parts.fracPart.empty()) {
    normalized.push_back('.');
    normalized += parts.fracPart;
  }
  return normalized;
}

int compareDecimalMagnitude(const DecimalParts &left, const DecimalParts &right) {
  if (left.intPart.size() != right.intPart.size()) {
    return left.intPart.size() < right.intPart.size() ? -1 : 1;
  }

  int intCmp = left.intPart.compare(right.intPart);
  if (intCmp != 0) {
    return intCmp < 0 ? -1 : 1;
  }

  size_t maxFrac = std::max(left.fracPart.size(), right.fracPart.size());
  for (size_t i = 0; i < maxFrac; ++i) {
    char leftDigit = i < left.fracPart.size() ? left.fracPart[i] : '0';
    char rightDigit = i < right.fracPart.size() ? right.fracPart[i] : '0';
    if (leftDigit != rightDigit) {
      return leftDigit < rightDigit ? -1 : 1;
    }
  }

  return 0;
}

int compareDecimalStrings(const std::string &left, const std::string &right) {
  DecimalParts leftParts;
  DecimalParts rightParts;
  if (!parseDecimalString(left, &leftParts) || !parseDecimalString(right, &rightParts)) {
    int cmp = strcmp(left.c_str(), right.c_str());
    if (cmp < 0) {
      return -1;
    }
    if (cmp > 0) {
      return 1;
    }
    return 0;
  }

  if (leftParts.negative != rightParts.negative) {
    return leftParts.negative ? -1 : 1;
  }

  int magnitude = compareDecimalMagnitude(leftParts, rightParts);
  if (leftParts.negative) {
    magnitude = -magnitude;
  }
  return magnitude;
}

}  // namespace

StateData::StateData()
{
  memset(this, 0, sizeof(StateData));
  type = en_column_data_null;
  calculateHash();
}

StateData::StateData(int64_t val):
    StateData()
{
    Set(val);
}

StateData::StateData(uint64_t val):
    StateData()
{
    Set(val);
}

StateData::StateData(double val):
    StateData()
{
    Set(val);
}

StateData::StateData(const std::string &val):
    StateData()
{
    Set(val.c_str(), val.size());
}

StateData::StateData(const StateData &c)
{
  Copy(c);
}

StateData::~StateData()
{
  Clear();
}

void StateData::Clear()
{
  if ((type == en_column_data_string || type == en_column_data_decimal) && d.str != nullptr)
  {
    free(d.str);
  }

  memset(this, 0, sizeof(StateData));
  str_len = 0;
  type = en_column_data_null;
  
  calculateHash();
}

void StateData::Copy(const StateData &c)
{
  is_subselect = c.is_subselect;
  is_equal = c.is_equal;
  type = c.type;
  str_len = c.str_len;
  if (type == en_column_data_string || type == en_column_data_decimal)
  {
    if (c.d.str != nullptr && str_len > 0) {
      d.str = static_cast<char *>(malloc(str_len + 1));
      memcpy(d.str, c.d.str, str_len);
      d.str[str_len] = 0;
    } else {
      d.str = nullptr;
    }
  }
  else
  {
    d = c.d;
  }
  
  _hash = c._hash;
}

bool StateData::SetData(en_state_log_column_data_type _type, void *_data, size_t _length)
{
  switch (_type & ~en_column_data_from_subselect)
  {
  case en_column_data_null:
    // debug("[StateData::SetData] en_column_data_null");
    break;

  case en_column_data_int:
    Set(*(int64_t *)_data);
    // debug("[StateData::SetData] en_column_data_int %ld", d.ival);
    break;

  case en_column_data_uint:
    Set(*(uint64_t *)_data);
    // debug("[StateData::SetData] en_column_data_uint %lu", d.uval);
    break;

  case en_column_data_double:
    Set(*(double *)_data);
    // debug("[StateData::SetData] en_column_data_double %f", d.fval);
    break;

  case en_column_data_string:
    Set((char *)_data, _length);
    // debug("[StateData::SetData] en_column_data_string %s", d.str);
    break;
  case en_column_data_decimal:
    SetDecimal((char *)_data, _length);
    break;

  default:
    // error("[StateData::SetData] data type parsing error");
    return false;
  }

  if (_type & en_column_data_from_subselect)
  {
    is_subselect = true;
    // debug("[StateData::SetData] is_subselect");
  }

  return true;
}

bool StateData::ConvertData(en_state_log_column_data_type _type)
{
  if (type == _type)
  {
    return true;
  }

  if (type == en_column_data_decimal || _type == en_column_data_decimal)
  {
    return false;
  }

  switch (_type)
  {
  case en_column_data_int:
  {
    int64_t value = 0;
    if (false == Get(value))
    {
      //데이터를 변환할 수 없음
      return false;
    }
    Set(value);
    return true;
  }

  case en_column_data_uint:
  {
    uint64_t value = 0;
    if (false == Get(value))
    {
      return false;
    }
    Set(value);
    return true;
  }

  case en_column_data_double:
  {
    double value = 0.f;
    if (false == Get(value))
    {
      return false;
    }
    Set(value);
    return true;
  }

  case en_column_data_string:
  {
    std::string value;
    if (false == Get(value))
    {
      return false;
    }
    Set(value.c_str(), value.size());
    return true;
  }
  case en_column_data_decimal:
    return false;

  default:
    return false;
  }
}

void StateData::SetEqual()
{
  is_equal = true;
  
  // calculateHash();
}

bool StateData::IsEqual() const
{
  return is_equal;
}

bool StateData::IsNone() const
{
  return type == en_column_data_null;
}

bool StateData::IsSubSelect() const
{
  return is_subselect;
}

en_state_log_column_data_type StateData::Type() const
{
  return type;
}

void StateData::Set(int64_t val)
{
  Clear();

  type = en_column_data_int;
  d.ival = val;
  
  calculateHash();
}

void StateData::Set(uint64_t val)
{
  Clear();

  type = en_column_data_uint;
  d.uval = val;
  
  calculateHash();
}

void StateData::Set(double val)
{
  Clear();

  type = en_column_data_double;
  d.fval = val;
  
  calculateHash();
}

void StateData::Set(const char *val, size_t length)
{
  Clear();

  type = en_column_data_string;
  d.str = (char *)malloc(length + 1);
  memcpy(d.str, val, length);
  d.str[length] = 0;
  str_len = length;
  
  calculateHash();
}

void StateData::SetDecimal(const std::string &val)
{
  SetDecimal(val.c_str(), val.size());
}

void StateData::SetDecimal(const char *val, size_t length)
{
  Clear();

  type = en_column_data_decimal;
  d.str = (char *)malloc(length + 1);
  memcpy(d.str, val, length);
  d.str[length] = 0;
  str_len = length;

  calculateHash();
}

bool StateData::Get(int64_t &val) const
{
  switch (type)
  {
  case en_column_data_int:
  case en_column_data_uint:
    val = d.ival;
    return true;

  case en_column_data_string:
    char *end;
    val = std::strtol(d.str, &end, 10);
    return true;
  case en_column_data_decimal:
    return false;

  case en_column_data_double:
  default:
    return false;
  }
}

bool StateData::Get(uint64_t &val) const
{
  switch (type)
  {
  case en_column_data_int:
  case en_column_data_uint:
    val = d.uval;
    return true;

  case en_column_data_string:
    char *end;
    val = std::strtoul(d.str, &end, 10);
    return true;
  case en_column_data_decimal:
    return false;

  case en_column_data_double:
  default:
    return false;
  }
}

bool StateData::Get(double &val) const
{
  switch (type)
  {
  case en_column_data_double:
    val = d.fval;
    return true;

  case en_column_data_string:
    char *end;
    val = std::strtold(d.str, &end);
    return true;
  case en_column_data_decimal:
    return false;

  case en_column_data_int:
  case en_column_data_uint:
  default:
    return false;
  }
}

bool StateData::Get(std::string &val) const
{
  switch (type)
  {
  case en_column_data_int:
    val = std::to_string(d.ival);
    return true;

  case en_column_data_uint:
    val = std::to_string(d.uval);
    return true;

  case en_column_data_double:
    val = std::to_string(d.fval);
    return true;

  case en_column_data_string:
    char *end;
    val = std::string(d.str);
    return true;
  case en_column_data_decimal:
    val = std::string(d.str, str_len);
    return true;

  default:
    return false;
  }
}

bool StateData::operator==(const StateData &c) const
{
  if (type != c.Type())
    return false;

#ifdef __amd64__
  static_assert(sizeof(UNION_RAW_DATA) == 8, "the size of UNION_RAW_DATA must be 8");
  
  if (type == en_column_data_string || type == en_column_data_decimal)
      return str_len == c.str_len && memcmp(d.str, c.d.str, str_len) == 0;

  return d.ival == c.d.ival;
#else
  switch (type)
  {
  case en_column_data_int:
    return d.ival == c.d.ival;

  case en_column_data_uint:
    return d.uval == c.d.uval;

  case en_column_data_double:
    return d.fval == c.d.fval;

  case en_column_data_string:
    return str_len == c.str_len && memcmp(d.str, c.d.str, str_len) == 0;
  case en_column_data_decimal:
    return str_len == c.str_len && memcmp(d.str, c.d.str, str_len) == 0;

  case en_column_data_null:
    return true;

  default:
    return false;
  }
#endif
}
bool StateData::operator!=(const StateData &c) const
{
  if (type != c.Type())
    return true;

#ifdef __amd64__
  static_assert(sizeof(UNION_RAW_DATA) == 8, "the size of UNION_RAW_DATA must be 8");
  
  if (type == en_column_data_string)
      return str_len != c.str_len || strcmp(d.str, c.d.str) != 0;
  if (type == en_column_data_decimal)
      return str_len != c.str_len || memcmp(d.str, c.d.str, str_len) != 0;
  
  return d.ival == c.d.ival;
#else
  switch (type)
  {
  case en_column_data_int:
    return d.ival != c.d.ival;

  case en_column_data_uint:
    return d.uval != c.d.uval;

  case en_column_data_double:
    return d.fval != c.d.fval;

  case en_column_data_string:
    return str_len != c.str_len || strcmp(d.str, c.d.str) != 0;
  case en_column_data_decimal:
    return str_len != c.str_len || memcmp(d.str, c.d.str, str_len) != 0;

  case en_column_data_null:
    return false;

  default:
    return true;
  }
#endif
}
bool StateData::operator>(const StateData &c) const
{
  if (type != c.Type())
    return false;

  switch (type)
  {
  case en_column_data_int:
    return d.ival > c.d.ival;

  case en_column_data_uint:
    return d.uval > c.d.uval;

  case en_column_data_double:
    return d.fval > c.d.fval;

  case en_column_data_string:
    return strcmp(d.str, c.d.str) > 0;
  case en_column_data_decimal:
    return false;

  default:
    return false;
  }
}
bool StateData::operator>=(const StateData &c) const
{
  if (type != c.Type())
    return false;

  switch (type)
  {
  case en_column_data_int:
    return d.ival >= c.d.ival;

  case en_column_data_uint:
    return d.uval >= c.d.uval;

  case en_column_data_double:
    return d.fval >= c.d.fval;

  case en_column_data_string:
      return strcmp(d.str, c.d.str) >= 0;
  case en_column_data_decimal:
      return false;


  default:
    return false;
  }
}
bool StateData::operator<(const StateData &c) const
{
  if (type != c.Type())
    return false;

  switch (type)
  {
  case en_column_data_int:
    return d.ival < c.d.ival;

  case en_column_data_uint:
    return d.uval < c.d.uval;

  case en_column_data_double:
    return d.fval < c.d.fval;

  case en_column_data_string:
      return strcmp(d.str, c.d.str) < 0;
  case en_column_data_decimal:
      return false;


  default:
    return false;
  }
}
bool StateData::operator<=(const StateData &c) const
{
  if (type != c.Type())
    return false;

  switch (type)
  {
  case en_column_data_int:
    return d.ival <= c.d.ival;

  case en_column_data_uint:
    return d.uval <= c.d.uval;

  case en_column_data_double:
    return d.fval <= c.d.fval;

  case en_column_data_string:
      return strcmp(d.str, c.d.str) <= 0;
  case en_column_data_decimal:
      return false;

  default:
    return false;
  }
}

StateData &StateData::operator=(const StateData &c)
{
  Clear();
  Copy(c);
  return *this;
}

void StateData::calculateHash() {
    static const size_t null_hash = std::hash<en_state_log_column_data_type>()(Type());
    
    if (Type() == en_column_data_null) {
        _hash = null_hash;
        return;
    }
    
    if (Type() == en_column_data_double) {
        _hash = (
            std::hash<en_state_log_column_data_type>()(Type()) ^
            (std::hash<decltype(d.fval)>()(d.fval) + 0x9e3779b9 + (_hash << 6) + (_hash >> 2))
        );
        return;
    }
    
    if (Type() == en_column_data_int) {
        _hash = (
            std::hash<en_state_log_column_data_type>()(Type()) ^
            (std::hash<decltype(d.ival)>()(d.ival) + 0x9e3779b9 + (_hash << 6) + (_hash >> 2))
        );
        return;
    }
    if (Type() == en_column_data_uint) {
        _hash = (
            std::hash<en_state_log_column_data_type>()(Type()) ^
            (std::hash<decltype(d.uval)>()(d.uval) + 0x9e3779b9 + (_hash << 6) + (_hash >> 2))
        );
        return;
    }
    
    if (Type() == en_column_data_string || Type() == en_column_data_decimal) {
        _hash = (
            std::hash<en_state_log_column_data_type>()(Type()) ^
            (std::hash<std::string>()(std::string(d.str, str_len)) + 0x9e3779b9 + (_hash << 6) + (_hash >> 2))
        );
        return;
    }
    
    // en_column_data_null
    _hash = null_hash;
}

std::size_t StateData::hash() const {
    return _hash;
}

StateRange::StateRange():
    range(std::make_shared<std::vector<ST_RANGE>>()),
    _wildcard(false),
    _hash(0)
{
    range->reserve(2);
}

StateRange::StateRange(int64_t singleValue):
    StateRange()
{
    SetValue(StateData { singleValue }, true);
}

StateRange::StateRange(const std::string &singleValue):
    StateRange()
{
    SetValue(StateData { singleValue }, true);
}

StateRange::~StateRange()
{
}

void StateRange::SetBegin(const StateData &_begin, bool _add_equal)
{
  ensureUniqueRange();
  range->emplace_back(ST_RANGE{_begin, StateData()});
  if (_add_equal)
    range->back().begin.SetEqual();
  
  calculateHash();
}

void StateRange::SetEnd(const StateData &_end, bool _add_equal)
{
  ensureUniqueRange();
  range->emplace_back(ST_RANGE{StateData(), _end});
  if (_add_equal)
    range->back().end.SetEqual();
    
  calculateHash();
}

void StateRange::SetBetween(const StateData &_begin, const StateData &_end)
{
  ensureUniqueRange();
  if (_begin < _end)
    range->emplace_back(ST_RANGE{_begin, _end});
  else
    range->emplace_back(ST_RANGE{_end, _begin});

  range->back().begin.SetEqual();
  range->back().end.SetEqual();
  
  calculateHash();
}

void StateRange::SetValue(const StateData &_value, bool _add_equal)
{
  ensureUniqueRange();
  if (_add_equal)
  {
    range->emplace_back(ST_RANGE{_value, _value});
    range->back().begin.SetEqual();
    range->back().end.SetEqual();
  }
  else
  {
    range->emplace_back(ST_RANGE{StateData(), _value});
    range->emplace_back(ST_RANGE{_value, StateData()});
  }
  
  calculateHash();
}

StateRange::EN_VALID StateRange::IsValid(const StateRange &a, const StateRange &b)
{
  if (a.range->size() > 0 || b.range->size() > 0)
  {
    return EN_VALID_RANGE;
  }

  return EN_VALID_NONE;
}

/**
 * @copilot this function checks if two ST_RANGEs are intersected.
 *   - begin.is_equal and end.is_equal must be considered for following reasons:
 *       - StateData::is_equal is a flag that indicates ~ than or equal to operator,
 *       - so if begin.is_equal is true, it means that begin is greater than or equal to.
 *       - if end.is_equal is true, it means that end is less than or equal to.
 */
bool StateRange::IsIntersection(const ST_RANGE &a, const ST_RANGE &b)
{
    const ST_RANGE *small = &a;
    const ST_RANGE *big = &b;
    
    if (a.begin > b.begin) {
        small = &b;
        big = &a;
    }
    
    if (small->end.IsNone() || big->begin.IsNone()) {
        return true;
    }
    
    return
        (small->end > big->begin) ||
        (small->end == big->begin && (small->end.IsEqual() || big->begin.IsEqual()));

}

/**
 * @copilot this function checks if two StateRange objects are equal.
 */
bool StateRange::operator==(const StateRange &c) const
{
    /*
    if (range->size() != c.range->size())
        return false;
    
    for (size_t idx = 0; idx < range->size(); ++idx)
    {
        if ((*range)[idx].begin.IsNone() != (*c.range)[idx].begin.IsNone())
            return false;
        
        if ((*range)[idx].begin.IsEqual() != (*c.range)[idx].begin.IsEqual())
            return false;
        
        if ((*range)[idx].begin != (*c.range)[idx].begin)
            return false;
        
        if ((*range)[idx].end != (*c.range)[idx].end)
            return false;
    }
    
    return true;
     */
    
    return std::hash<StateRange>()(*this) == std::hash<StateRange>()(c);
}

bool StateRange::operator!=(const StateRange &other) const {
    return std::hash<StateRange>()(*this) != std::hash<StateRange>()(other);
}

bool StateRange::operator<(const StateRange &other) const {
    // @copilot:
    //   Q: i have defined operator< to use StateRange as key of std::map.
    //      is it ok to use std::hash<StateRange>()(*this) < std::hash<StateRange>()(other) ?
    //   A: yes, it is ok.
    //   OK, thanks.
    
    return std::hash<StateRange>()(*this) < std::hash<StateRange>()(other);
}

bool StateRange::wildcard() const {
    return _wildcard;
}

void StateRange::setWildcard(bool wildcard) {
    _wildcard = wildcard;
    
    calculateHash();
}

std::string StateRange::MakeWhereQuery() {
    return MakeWhereQuery("FIXME");
}

std::string StateRange::MakeWhereQuery(std::string columnName) const
{
  std::string full_name = columnName;
  // auto pos = full_name.find_last_of('.');
  // std::string key_name = full_name.substr(pos + 1);
  const std::string &key_name = full_name;

  std::string val1;
  std::string val2;

  if (range->size() > 0)
  {
    std::stringstream ss;

    for (auto &i : *range)
    {
      if (i.begin.IsNone() && i.end.IsNone())
      {
        continue;
      }
      else if (i.begin.IsNone() && !i.end.IsNone())
      {
        val1 = formatStateDataForWhere(i.end);
        if (i.end.IsEqual())
        {
          ss << key_name << "<=" << val1 << " OR ";
        }
        else
        {
          ss << key_name << "<" << val1 << " OR ";
        }
      }
      else if (!i.begin.IsNone() && i.end.IsNone())
      {
        val1 = formatStateDataForWhere(i.begin);
        if (i.begin.IsEqual())
        {
          ss << key_name << ">=" << val1 << " OR ";
        }
        else
        {
          ss << key_name << ">" << val1 << " OR ";
        }
      }
      else //(!i.begin.IsNone() && !i.end.IsNone())
      {
        val1 = formatStateDataForWhere(i.begin);
        val2 = formatStateDataForWhere(i.end);

        if (val1 == val2)
        {
          if (i.begin.IsEqual())
          {
            ss << key_name << "=" << val1 << " OR ";
          }
          else
          {
            ss << key_name << "!=" << val1 << " OR ";
          }
        }
        else
        {
          ss << "(";
          if (i.begin.IsEqual())
          {
            ss << key_name << ">=" << val1 << " AND ";
          }
          else
          {
            ss << key_name << ">" << val1 << " AND ";
          }
          if (i.end.IsEqual())
          {
            ss << key_name << "<=" << val2;
          }
          else
          {
            ss << key_name << "<" << val2;
          }
          ss << ") OR ";
        }
      }
    }

    std::string ret = ss.str();
    auto pos = ret.rfind(" OR ");

    return ret.substr(0, pos);
  }
  else
  {
    //where 절이 필요하지 않음
    return std::string();
  }
}

void StateRange::SetValues(const StateData &_values) {
}

const std::vector<StateRange::ST_RANGE> *StateRange::GetRange() const
{
  return range.get();
}

std::shared_ptr<std::vector<StateRange>> StateRange::OR_ARRANGE(const std::vector<StateRange> &a)
{
  StateRange range;
  
  for (auto &i : a)
  {
    range.range->insert(range.range->end(), i.range->begin(), i.range->end());
    range._wildcard |= i.wildcard();
  }
  range.range = OR_ARRANGE(range.range);
  
  range.calculateHash();

  auto vec = std::make_shared<std::vector<StateRange>>();

  if (range.range->size() > 0)
  {
    vec->emplace_back(range);
    return vec;
  }
  else
  {
    return vec;
  }
}

/**
 * @copilot this function performs OR operation between two StateRange objects.
 */
bool StateRange::isIntersects(const StateRange &a, const StateRange &b) {
    if (IsValid(a, b) != EN_VALID_RANGE) {
        return false;
    }
    
    if ((a == b) || (a.wildcard() || b.wildcard())) {
        return true;
    }
    
    
    const auto &range1 = *a.range;
    const auto &range2 = *b.range;
    
    auto intersectionExists = [&](const auto& i) {
        return std::any_of(range2.begin(), range2.end(), [&](const auto& j) {
            return IsIntersection(i, j);
        });
    };
    
    return std::any_of(range1.begin(), range1.end(), intersectionExists);
}

/**
 * @copilot this function performs AND operation between two StateRange objects.
 *  - this function is used for merging two StateRange objects. (e.g. a = a & b)
 */
std::shared_ptr<StateRange> StateRange::AND(const StateRange &a, const StateRange &b)
{
    auto range = std::make_shared<StateRange>();
    
    if (IsValid(a, b) != EN_VALID_RANGE) {
        return range;
    }

    // wildcard에 대한 교집합 연산은 wildcard가 아닌 반대편의 범위값이어야 함
    if (a.wildcard()) {
        return std::make_shared<StateRange>(b);
    } else if (b.wildcard()) {
        return std::make_shared<StateRange>(a);
    }
    
    // merge two ST_RANGEs until it is not possible to merge
    auto range1 = *a.range;
    auto range2 = *b.range;
    
    range->range->reserve(range1.size() + range2.size());
    
    while (range1.size() > 0 && range2.size() > 0) {
        auto &i = range1.front();
        auto &j = range2.front();
        
        if (IsIntersection(i, j)) {
            range->range->emplace_back(std::move(i & j));
            range1.erase(range1.begin());
            range2.erase(range2.begin());
        } else if (i.begin.IsNone()) {
            range->range->emplace_back(std::move(j));
            range2.erase(range2.begin());
        } else if (j.begin.IsNone()) {
            range->range->emplace_back(std::move(i));
            range1.erase(range1.begin());
        } else {
            if (i.begin < j.begin) {
                range->range->emplace_back(std::move(i));
                range1.erase(range1.begin());
            } else {
                range->range->emplace_back(std::move(j));
                range2.erase(range2.begin());
            }
        }
    }
    
    range->calculateHash();
  
    return range;
}

/**
 * @copilot this function performs OR operation between two StateRange objects.
 * - this function is used for merging two StateRange objects. (e.g. a = a | b)
 */
void StateRange::OR_FAST(const StateRange &b, bool ignoreIntersect) {
    if (*this == b) {
        return;
    }
    
    if (IsValid(*this, b) != EN_VALID_RANGE) {
        return;
    }
    
    if (b.wildcard()) {
        *this = b;
        return;
    }

    ensureUniqueRange();
    
    auto &range1 = *range;
    const auto &range2 = *b.range;
    
    range1.reserve(range1.size() + range2.size());
    
    // merge two ST_RANGEs until it is not possible to merge
    for (int x = 0; x < range2.size(); x++) {
        const auto &j = range2[x];
        
        bool is_merged = false;
        for (auto &i: range1) {
            if (ignoreIntersect || IsIntersection(i, j)) {
                i = std::move(i | j);
                is_merged = true;
                break;
            }
        }
        
        if (!is_merged) {
            range1.emplace_back(j);
        }
        
        // range2.erase(range2.begin());
    }
    
    calculateHash();
    
    true;
}

std::shared_ptr<StateRange> StateRange::OR(const StateRange &a, const StateRange &b, bool ignoreIntersect)
{
    if (IsValid(a, b) != EN_VALID_RANGE) {
        return std::make_shared<StateRange>();
    }
    
    auto range = std::make_shared<StateRange>(a);
    range->OR_FAST(b, ignoreIntersect);
    
    return std::move(range);
}


/**
 * @deprecated use isIntersects() instead.
 */
std::shared_ptr<std::vector<StateRange::ST_RANGE>> StateRange::AND(const ST_RANGE &a, const ST_RANGE &b)
{
  auto new_range = std::make_shared<std::vector<ST_RANGE>>();
  const ST_RANGE *small, *big;

  //a.begin 이 더 작을경우
  if (Min(a.begin, b.begin) == 0)
  {
    small = &a;
    big = &b;
  }
  //b.begin 이 더 작을경우 (또는 완벽히 동일할 경우)
  else
  {
    small = &b;
    big = &a;
  }

  if (IsIntersection(*small, *big))
  {
    //교집합
    if (big->begin == big->end)
    {
      new_range->emplace_back(*big);
    }
    else
    {
      new_range->emplace_back(ST_RANGE{big->begin, small->end});
    }
  }
  else
  {
    //공집합
  }

  return new_range;
}

std::shared_ptr<std::vector<StateRange::ST_RANGE>> StateRange::OR_ARRANGE(const std::shared_ptr<std::vector<ST_RANGE>> a) {
  if (a->size() < 2)
    return std::make_shared<std::vector<ST_RANGE>>(*a);

  // TODO: 첫번째 iteration에서는 페어끼리 비교해서 graph 를만들고
  // TODO: 두번쨰 iteration에서는 그래프를 비교함
  
  // 가지고 있는 범위 데이터를 재정렬
  // 합칠수 있으면 합침
  std::shared_ptr<std::vector<ST_RANGE>> curr_range = std::make_shared<std::vector<ST_RANGE>>(*a);
  std::shared_ptr<std::vector<ST_RANGE>> new_range = std::make_shared<std::vector<ST_RANGE>>();

  bool is_change = true;
  while (is_change)
  {
    new_range->clear();
    is_change = false;

    auto curr = (*curr_range)[0];
    for (size_t i = 1; i < curr_range->size(); ++i)
    {
      auto ret = OR(curr, (*curr_range)[i]);
      if (ret->size() > 0)
      {
        is_change = true;
        curr = (*ret)[0];
      }
      else
      {
        new_range->emplace_back(curr);
        curr = (*curr_range)[i];
      }
    }
    new_range->emplace_back(curr);
    curr_range = std::make_shared<std::vector<ST_RANGE>>(*new_range);
  }
  

  return std::move(new_range);
}

/**
 * @copilot simplified version of OR_ARRANGE().
 *
 * - this function is used for arranging range objects of StateRange::range.
 * - this function visits all range objects and merges them if they are intersected.
 * - previous implementation of OR_ARRANGE() was too inefficient:
 *      - it creates new vector and copies all range objects to it every iteration.
 *      - it uses OR() function to merge two range objects, which is too inefficient. (TODO: use ST_RANGE::operator|)
 * - above problems must be solved and this function must be optimized.
 */
std::shared_ptr<std::vector<StateRange::ST_RANGE>> StateRange::OR_ARRANGE2(const std::shared_ptr<std::vector<ST_RANGE>> a) {
    if (a->size() < 2)
        return std::make_shared<std::vector<ST_RANGE>>(*a);
    
    
    // visit all range objects and merge them if they are intersected.
    std::shared_ptr<std::vector<ST_RANGE>> curr_range = std::make_shared<std::vector<ST_RANGE>>(*a);
    std::shared_ptr<std::vector<ST_RANGE>> new_range = std::make_shared<std::vector<ST_RANGE>>();
    
    while (curr_range->size() > 0) {
        auto curr = (*curr_range)[0];
        curr_range->erase(curr_range->begin());
        
        for (auto it = curr_range->begin(); it != curr_range->end(); ) {
            if (IsIntersection(curr, *it)) {
                curr = curr | *it;
                it = curr_range->erase(it);
            } else {
                ++it;
            }
        }
        
        new_range->emplace_back(curr);
    }
    
    return new_range;
}

std::shared_ptr<std::vector<StateRange::ST_RANGE>> StateRange::OR(const ST_RANGE &a, const ST_RANGE &b)
{
    if (IsIntersection(a, b)) {
        return std::shared_ptr<std::vector<StateRange::ST_RANGE>>(
            new std::vector<StateRange::ST_RANGE>{a | b}
        );
    }
    
    return std::make_shared<std::vector<StateRange::ST_RANGE>>();
}

// a 가 작으면 : 0
// b 가 작으면 : 1
int StateRange::Min(const StateData &a, const StateData &b)
{
  if (a.IsNone())
    return 0;
  if (b.IsNone())
    return 1;
  if (a < b)
    return 0;
  //같으면 Equal 을 포함한쪽이 더 작음
  if (a == b && a.IsEqual())
    return 0;
  return 1;
}

// a 가 크면 : 0
// b 가 크면 : 1
int StateRange::Max(const StateData &a, const StateData &b)
{
  if (a.IsNone())
    return 0;
  if (b.IsNone())
    return 1;
  if (a > b)
    return 0;
  //같으면 Equal 을 포함한쪽이 더 큼
  if (a == b && a.IsEqual())
    return 0;
  return 1;
}

void StateRange::arrangeSelf() {
    range = OR_ARRANGE2(range);
    
    calculateHash();
}

void StateRange::ensureUniqueRange() {
  if (!range) {
    range = std::make_shared<std::vector<ST_RANGE>>();
    range->reserve(2);
    return;
  }

  if (range.use_count() > 1) {
    range = std::make_shared<std::vector<ST_RANGE>>(*range);
  }
}

void StateRange::calculateHash() {
    std::size_t hash = 0;
    
    if (wildcard()) {
        _hash = (std::size_t) UINT64_MAX;
        return;
    }
    
    for (const auto &st_range: *range) {
        /*
         * @copilot: please improve this hash function.
         * this will make collision when the range is like:
         *  (st_range.begin = 1, st_range.end = 2)
         *  (st_range.begin = 2, st_range.end = 1)
         *
         *  the hash will be the same.
         */
        
        hash ^= std::hash<StateData>()(st_range.begin) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
        // this will make the hash function better.
        hash ^= std::hash<StateData>()(st_range.end) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    
    _hash = hash;
}

std::size_t StateRange::hash() const {
    return _hash;
}

StateItem::StateItem():
    condition_type(EN_CONDITION_NONE),
    function_type(FUNCTION_NONE),
    _isRangeCacheBuilt(false)
{
}

/**
 * COPY CONSTRUCTOR
 *
 * @param other
 */
StateItem::StateItem(const StateItem &other):
    condition_type(other.condition_type),
    function_type(other.function_type),
    name(other.name),
    arg_list(other.arg_list),
    data_list(other.data_list),
    _rangeCache(other._rangeCache),
    _isRangeCacheBuilt(other._isRangeCacheBuilt)
{

}

StateItem::~StateItem()
{
}

bool StateItem::is_data_ok(const StateItem &item)
{
  if (item.arg_list.size() > 0)
  {
    //SUB SELECT 는 쿼리에서 값을 뽑아낼 수 없음
    return false;
  }

  switch (item.function_type)
  {
  case FUNCTION_BETWEEN:
    if (item.data_list.size() != 2)
      return false;
    else
      return true;

  case FUNCTION_EQ:
  case FUNCTION_NE:
  case FUNCTION_LT:
  case FUNCTION_LE:
  case FUNCTION_GT:
  case FUNCTION_GE:
    if (item.data_list.size() != 1)
      return false;
    else
      return true;
    
  case FUNCTION_WILDCARD:
    return true;

  default:
    return false;
  }
}


std::shared_ptr<StateRange> StateItem::MakeRange()
{
  return MakeRange(*this);
}

bool RangeRecursive(StateItem &item, const std::string &column_name, bool &is_valid)
{
  if (item.arg_list.size() > 0)
  {
    for (auto iter = item.arg_list.begin(); iter != item.arg_list.end(); )
    {
      // 타겟 컬럼이 아니면 list 에서 삭제
      if (RangeRecursive(*iter, column_name, is_valid))
      {
        // 삭제된 컬럼이 OR 연산인 경우 해당 쿼리는 전체 선택이 될 수 있기에 valid 하지 않음
        if (item.condition_type == EN_CONDITION_OR)
        {
          is_valid = false;
        }

        iter = item.arg_list.erase(iter);
      }
      else
      {
        iter++;
      }
    }

    if (item.arg_list.size() == 0)
    {
      // list 가 전부 삭제 되면 상위 item 도 삭제 되어야 함
      return true;
    }
  }

  if (item.sub_query_list.size() > 0)
  {
    for (auto iter = item.sub_query_list.begin(); iter != item.sub_query_list.end(); )
    {
      // 타겟 컬럼이 아니면 list 에서 삭제
      if (RangeRecursive(*iter, column_name, is_valid))
      {
        iter = item.sub_query_list.erase(iter);
      }
      else
      {
        iter++;
      }
    }

    if (item.sub_query_list.size() == 0)
    {
      // list 가 전부 삭제 되면 상위 item 도 삭제 되어야 함
      return true;
    }
  }

  if (item.name.size() == 0)
    return false;
  
  if (item.name != column_name)
  {
    // 타겟 컬럼이 아님
    return true;
  }

  return false;
}

std::shared_ptr<StateRange> StateItem::MakeRange(const std::string &column_name, bool &is_valid)
{
  RangeRecursive(*this, column_name, is_valid);
  return MakeRange(*this);
}

/**
 * @brief
 * @copilot please implement this function:
 *
 * - when condition_type is not EN_CONDITION_NONE (this means this item is a AND / OR condition),
 *   you must call MakeRange2() for each item in arg_list, and then call AND / OR function of StateRange
 *   to combine all the ranges.
 *
 *   for example (pseudo code):
 *      std::vector<StateRange> ranges;
 *      StateRange output;
 *      std::transform(arg_list.begin(), arg_list.end(), std::back_inserter(ranges), [](const StateItem &item) { item.MakeRange2(); });
 *
 *      if (condition_type == EN_CONDITION_AND) {
 *          output = ranges.shift();
 *          for (auto &range : ranges) {
 *              output = StateRange::AND(output, range);
*           }
 *      } else if (condition_type == EN_CONDITION_OR) {
 *          output = ranges.shift();
 *          for (auto &range : ranges) {
 *              output = StateRange::OR(output, range);
 *          }
 *      }
 *
 *
 */
const StateRange &StateItem::MakeRange2() const {
    if (_isRangeCacheBuilt) {
        return _rangeCache;
    }
    
    if (condition_type != EN_CONDITION_NONE) {
        std::vector<StateRange> ranges;
        StateRange output;
        
        std::transform(
            arg_list.begin(), arg_list.end(),
            std::back_inserter(ranges),
            [](const StateItem &item) { return item.MakeRange2(); }
        );
        
        if (condition_type == EN_CONDITION_AND) {
            output = ranges.front();
            ranges.erase(ranges.begin());
            
            for (auto &range : ranges) {
                output = *StateRange::AND(output, range);
            }
        } else if (condition_type == EN_CONDITION_OR) {
            output = ranges.front();
            ranges.erase(ranges.begin());
            
            for (auto &range : ranges) {
                output = *StateRange::OR(output, range);
            }
        }
        
        _isRangeCacheBuilt = true;
        _rangeCache = output;
        
        _rangeCache.calculateHash();
    } else {
        StateRange range;
        
        if (!data_list.empty()) {
            switch (function_type) {
                case FUNCTION_BETWEEN:
                    range.SetBetween(data_list[0], data_list[1]);
                    break;
                
                case FUNCTION_EQ:
                    range.SetValue(data_list[0], true);
                    break;
                
                case FUNCTION_NE:
                    range.SetValue(data_list[0], false);
                    break;
                
                case FUNCTION_LT:
                    range.SetEnd(data_list[0], false);
                    break;
                
                case FUNCTION_LE:
                    range.SetEnd(data_list[0], true);
                    break;
                
                case FUNCTION_GT:
                    range.SetBegin(data_list[0], false);
                    break;
                
                case FUNCTION_GE:
                    range.SetBegin(data_list[0], true);
                    break;
                case FUNCTION_IN_INTERNAL:
                    for (auto &data : data_list) {
                        range.SetValue(data, true);
                    }
                    break;
                case FUNCTION_WILDCARD:
                    range.setWildcard(true);
                    break;
                    
                default:
                    break;
            }
        }
        
        _isRangeCacheBuilt = true;
        _rangeCache = range;
        
        _rangeCache.calculateHash();
    }
    
    return _rangeCache;
}

std::shared_ptr<StateRange> StateItem::MakeRange(const StateItem &item) {
    
    if (item.condition_type != EN_CONDITION_NONE)
    {
        if (item.arg_list.size() == 1)
        {
            return MakeRange(item.arg_list[0]);
        }
        
        switch (item.condition_type)
        {
            case EN_CONDITION_AND:
                if (item.arg_list.size() > 1)
                {
                    auto range = StateRange::AND(*MakeRange(item.arg_list[0]), *MakeRange(item.arg_list[1]));
                    
                    for (size_t i = 2; i < item.arg_list.size(); ++i)
                    {
                        range = StateRange::AND(*range, *MakeRange(item.arg_list[i]));
                    }
                    
                    return range;
                }
                break;
            
            case EN_CONDITION_OR:
                if (item.arg_list.size() > 1)
                {
                    auto range = StateRange::OR(*MakeRange(item.arg_list[0]), *MakeRange(item.arg_list[1]));
                    
                    for (size_t i = 2; i < item.arg_list.size(); ++i)
                    {
                        range = StateRange::OR(*range, *MakeRange(item.arg_list[i]));
                    }
                    
                    return range;
                }
                break;
            
            default:
                break;
        }
        
        return std::make_shared<StateRange>();
    }
    
    if (item.function_type != FUNCTION_NONE && is_data_ok(item) == true)
    {
        auto range = std::make_shared<StateRange>();
        
        switch (item.function_type)
        {
            case FUNCTION_BETWEEN:
                range->SetBetween(item.data_list[0], item.data_list[1]);
                return range;
            
            case FUNCTION_EQ:
                range->SetValue(item.data_list[0], true);
                return range;
            
            case FUNCTION_NE:
                range->SetValue(item.data_list[0], false);
                return range;
            
            case FUNCTION_LT:
                range->SetEnd(item.data_list[0], false);
                return range;
            
            case FUNCTION_LE:
                range->SetEnd(item.data_list[0], true);
                return range;
            
            case FUNCTION_GT:
                range->SetBegin(item.data_list[0], false);
                return range;
            
            case FUNCTION_GE:
                range->SetBegin(item.data_list[0], true);
                return range;
            
            case FUNCTION_WILDCARD:
                range->setWildcard(true);
                return range;
            
            default:
                break;
        }
        
        return std::make_shared<StateRange>();
    }
    
    if (item.condition_type == EN_CONDITION_NONE && item.function_type == FUNCTION_NONE)
    {
        if (item.data_list.size() == 1)
        {
            auto range = std::make_shared<StateRange>();
            range->SetValue(item.data_list[0], true);
            return range;
        }
    }
    
    return std::make_shared<StateRange>();
}

StateItem StateItem::EQ(const std::string &name, const StateData &data) {
    StateItem item;
    item.name = name;
    item.function_type = FUNCTION_EQ;
    
    item.data_list.emplace_back(data);
    
    return item;
}

StateItem StateItem::Wildcard(const std::string &name) {
    StateItem item;
    item.name = name;
    item.function_type = FUNCTION_WILDCARD;
    
    return item;
}

void StateData::toProtobuf(ultraverse::state::v2::proto::StateData *out) const {
    if (out == nullptr) {
        return;
    }

    out->Clear();
    out->set_is_subselect(is_subselect);
    out->set_is_equal(is_equal);
    out->set_type(static_cast<uint32_t>(type));
    out->set_hash(static_cast<uint64_t>(_hash));

    switch (type) {
        case en_column_data_int:
            out->set_int_value(d.ival);
            break;
        case en_column_data_uint:
            out->set_uint_value(d.uval);
            break;
        case en_column_data_double:
            out->set_double_value(d.fval);
            break;
        case en_column_data_string:
        case en_column_data_decimal:
            if (d.str != nullptr && str_len > 0) {
                out->set_string_value(std::string(d.str, str_len));
            } else {
                out->set_string_value("");
            }
            break;
        case en_column_data_null:
        case en_column_data_from_subselect:
        default:
            break;
    }
}

void StateData::fromProtobuf(const ultraverse::state::v2::proto::StateData &msg) {
    Clear();

    const auto dataType = static_cast<en_state_log_column_data_type>(msg.type());
    switch (dataType) {
        case en_column_data_int:
            Set(static_cast<int64_t>(msg.int_value()));
            break;
        case en_column_data_uint:
            Set(static_cast<uint64_t>(msg.uint_value()));
            break;
        case en_column_data_double:
            Set(static_cast<double>(msg.double_value()));
            break;
        case en_column_data_string: {
            const auto &value = msg.string_value();
            Set(value.c_str(), value.size());
            break;
        }
        case en_column_data_decimal: {
            const auto &value = msg.string_value();
            SetDecimal(value.c_str(), value.size());
            break;
        }
        case en_column_data_null:
        case en_column_data_from_subselect:
        default:
            Clear();
            break;
    }

    is_subselect = msg.is_subselect();
    is_equal = msg.is_equal();
    type = dataType;
    _hash = static_cast<std::size_t>(msg.hash());
}

void StateRange::ST_RANGE::toProtobuf(ultraverse::state::v2::proto::StateRangeInterval *out) const {
    if (out == nullptr) {
        return;
    }

    out->Clear();
    begin.toProtobuf(out->mutable_begin());
    end.toProtobuf(out->mutable_end());
}

void StateRange::ST_RANGE::fromProtobuf(const ultraverse::state::v2::proto::StateRangeInterval &msg) {
    begin.fromProtobuf(msg.begin());
    end.fromProtobuf(msg.end());
}

void StateRange::toProtobuf(ultraverse::state::v2::proto::StateRange *out) const {
    if (out == nullptr) {
        return;
    }

    out->Clear();
    out->set_hash(static_cast<uint64_t>(_hash));

    if (!range) {
        return;
    }

    for (const auto &entry : *range) {
        auto *interval = out->add_range();
        entry.toProtobuf(interval);
    }
}

void StateRange::fromProtobuf(const ultraverse::state::v2::proto::StateRange &msg) {
    range = std::make_shared<std::vector<ST_RANGE>>();
    range->reserve(static_cast<size_t>(msg.range_size()));
    _wildcard = false;
    _hash = static_cast<std::size_t>(msg.hash());

    for (const auto &interval : msg.range()) {
        ST_RANGE entry;
        entry.fromProtobuf(interval);
        range->push_back(std::move(entry));
    }
}

void StateItem::toProtobuf(ultraverse::state::v2::proto::StateItem *out) const {
    if (out == nullptr) {
        return;
    }

    out->Clear();
    out->set_condition_type(static_cast<uint32_t>(condition_type));
    out->set_function_type(static_cast<uint32_t>(function_type));
    out->set_name(name);

    for (const auto &arg : arg_list) {
        auto *argMsg = out->add_arg_list();
        arg.toProtobuf(argMsg);
    }

    for (const auto &data : data_list) {
        auto *dataMsg = out->add_data_list();
        data.toProtobuf(dataMsg);
    }

    for (const auto &subQuery : sub_query_list) {
        auto *subMsg = out->add_sub_query_list();
        subQuery.toProtobuf(subMsg);
    }

    _rangeCache.toProtobuf(out->mutable_range_cache());
    out->set_is_range_cache_built(_isRangeCacheBuilt);
}

void StateItem::fromProtobuf(const ultraverse::state::v2::proto::StateItem &msg) {
    condition_type = static_cast<EN_CONDITION_TYPE>(msg.condition_type());
    function_type = static_cast<EN_FUNCTION_TYPE>(msg.function_type());
    name = msg.name();

    arg_list.clear();
    arg_list.reserve(static_cast<size_t>(msg.arg_list_size()));
    for (const auto &argMsg : msg.arg_list()) {
        StateItem arg;
        arg.fromProtobuf(argMsg);
        arg_list.emplace_back(std::move(arg));
    }

    data_list.clear();
    data_list.reserve(static_cast<size_t>(msg.data_list_size()));
    for (const auto &dataMsg : msg.data_list()) {
        StateData data;
        data.fromProtobuf(dataMsg);
        data_list.emplace_back(std::move(data));
    }

    sub_query_list.clear();
    sub_query_list.reserve(static_cast<size_t>(msg.sub_query_list_size()));
    for (const auto &subMsg : msg.sub_query_list()) {
        StateItem sub;
        sub.fromProtobuf(subMsg);
        sub_query_list.emplace_back(std::move(sub));
    }

    _rangeCache.fromProtobuf(msg.range_cache());
    _isRangeCacheBuilt = msg.is_range_cache_built();
}
