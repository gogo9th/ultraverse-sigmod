#include "StateItem.h"

#include <sstream>

StateData::StateData()
{
  memset(this, 0, sizeof(StateData));
  type = en_column_data_null;
}

StateData::StateData(const StateData &c)
{
  memset(this, 0, sizeof(StateData));
  type = en_column_data_null;

  Copy(c);
}

StateData::~StateData()
{
  Clear();
}

void StateData::Clear()
{
  if (type == en_column_data_string && d.str != nullptr)
  {
    free(d.str);
  }

  memset(this, 0, sizeof(StateData));
  type = en_column_data_null;
}

void StateData::Copy(const StateData &c)
{
  is_equal = c.is_equal;
  type = c.type;
  if (type == en_column_data_string)
  {
    d.str = strdup(c.d.str);
  }
  else
  {
    d = c.d;
  }
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

  default:
    return false;
  }
}

void StateData::SetEqual()
{
  is_equal = true;
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
}

void StateData::Set(uint64_t val)
{
  Clear();

  type = en_column_data_uint;
  d.uval = val;
}

void StateData::Set(double val)
{
  Clear();

  type = en_column_data_double;
  d.fval = val;
}

void StateData::Set(const char *val, size_t length)
{
  Clear();

  type = en_column_data_string;
  d.str = (char *)malloc(length + 1);
  memcpy(d.str, val, length);
  d.str[length] = 0;
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

  default:
    return false;
  }
}

bool StateData::operator==(const StateData &c) const
{
  if (type != c.Type())
    return false;

  switch (type)
  {
  case en_column_data_int:
    return d.ival == c.d.ival;

  case en_column_data_uint:
    return d.uval == c.d.uval;

  case en_column_data_double:
    return d.fval == c.d.fval;

  case en_column_data_string:
    return strcmp(d.str, c.d.str) == 0;

  case en_column_data_null:
    return true;

  default:
    return false;
  }
}
bool StateData::operator!=(const StateData &c) const
{
  if (type != c.Type())
    return true;

  switch (type)
  {
  case en_column_data_int:
    return d.ival != c.d.ival;

  case en_column_data_uint:
    return d.uval != c.d.uval;

  case en_column_data_double:
    return d.fval != c.d.fval;

  case en_column_data_string:
    return strcmp(d.str, c.d.str) != 0;

  case en_column_data_null:
    return false;

  default:
    return true;
  }
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
    return std::string(d.str) > c.d.str;

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
    return std::string(d.str) >= c.d.str;

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
    return std::string(d.str) < c.d.str;

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
    return std::string(d.str) <= c.d.str;

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

StateRange::StateRange()
{
}

StateRange::~StateRange()
{
}

void StateRange::SetBegin(const StateData &_begin, bool _add_equal)
{
  range.emplace_back(ST_RANGE{_begin, StateData()});
  if (_add_equal)
    range.back().begin.SetEqual();
}

void StateRange::SetEnd(const StateData &_end, bool _add_equal)
{
  range.emplace_back(ST_RANGE{StateData(), _end});
  if (_add_equal)
    range.back().end.SetEqual();
}

void StateRange::SetBetween(const StateData &_begin, const StateData &_end)
{
  if (_begin < _end)
    range.emplace_back(ST_RANGE{_begin, _end});
  else
    range.emplace_back(ST_RANGE{_end, _begin});

  range.back().begin.SetEqual();
  range.back().end.SetEqual();
}

void StateRange::SetValue(const StateData &_value, bool _add_equal)
{
  if (_add_equal)
  {
    range.emplace_back(ST_RANGE{_value, _value});
    range.back().begin.SetEqual();
    range.back().end.SetEqual();
  }
  else
  {
    range.emplace_back(ST_RANGE{StateData(), _value});
    range.emplace_back(ST_RANGE{_value, StateData()});
  }
}

StateRange::EN_VALID StateRange::IsValid(const StateRange &a, const StateRange &b)
{
  if (a.range.size() > 0 || b.range.size() > 0)
  {
    return EN_VALID_RANGE;
  }

  return EN_VALID_NONE;
}

bool StateRange::IsIntersection(const ST_RANGE &small, const ST_RANGE &big)
{
  //small.end 와 big.begin 이 겹치는가
  //small.end 와 big.begin 이 동일하면 Equal 이 있을경우 겹침
  if (small.end.IsNone() || big.begin.IsNone() ||
      small.end > big.begin ||
      (small.end == big.begin && (small.end.IsEqual() || big.begin.IsEqual())))
  {
    return true;
  }
  else
  {
    return false;
  }
}

bool StateRange::operator==(const StateRange &c) const
{
  if (range.size() != c.range.size())
    return false;

  for (size_t idx = 0; idx < range.size(); ++idx)
  {
    if (range[idx].begin != c.range[idx].begin ||
        range[idx].end != c.range[idx].end)
    {
      return false;
    }
  }

  return true;
}

std::string StateRange::MakeWhereQuery() {
    return MakeWhereQuery("FIXME");
}

std::string StateRange::MakeWhereQuery(std::string columnName)
{
  std::string full_name = columnName;
  auto pos = full_name.find_last_of('.');
  std::string key_name = full_name.substr(pos + 1);

  std::string val1;
  std::string val2;

  if (range.size() > 0)
  {
    std::stringstream ss;

    for (auto &i : range)
    {
      if (i.begin.IsNone() && i.end.IsNone())
      {
        continue;
      }
      else if (i.begin.IsNone() && !i.end.IsNone())
      {
        i.end.Get(val1);
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
        i.begin.Get(val1);
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
        i.begin.Get(val1);
        i.end.Get(val2);

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

const std::vector<StateRange::ST_RANGE> *StateRange::GetRange() const
{
  return &range;
}

std::vector<StateRange> StateRange::OR_ARRANGE(const std::vector<StateRange> &a)
{
  StateRange range;
  for (auto &i : a)
  {
    range.range.insert(range.range.end(), i.range.begin(), i.range.end());
  }
  range.range = OR_ARRANGE(range.range);

  std::vector<StateRange> vec;

  if (range.range.size() > 0)
  {
    vec.emplace_back(range);
    return vec;
  }
  else
  {
    return vec;
  }
}

StateRange StateRange::AND(const StateRange &a, const StateRange &b)
{
  StateRange range;

  auto ret = IsValid(a, b);

  if (ret == EN_VALID_RANGE)
  {
    auto range1 = OR_ARRANGE(a.range);
    auto range2 = OR_ARRANGE(b.range);

    for (auto &i : range1)
    {
      for (auto &j : range2)
      {
        auto ret = AND(i, j);
        if (ret.size() > 0)
        {
          range.range.emplace_back(ret[0]);
          break;
        }
      }
    }
    return range;
  }
  else
  {
    //정보가 비어 있음
    return range;
  }
}

StateRange StateRange::OR(const StateRange &a, const StateRange &b)
{
  StateRange range;

  auto ret = IsValid(a, b);

  if (ret == EN_VALID_RANGE)
  {
    range.range.insert(range.range.end(), a.range.begin(), a.range.end());
    range.range.insert(range.range.end(), std::find_if(b.range.begin(), b.range.end(), [&range](auto &r) {
        return std::find(range.range.begin(), range.range.end(), r) == range.range.end();
    }), b.range.end());
    // range.range.insert(range.range.end(), b.range.begin(), b.range.end());
    range.range = OR_ARRANGE(range.range);
    return range;
  }
  else
  {
    //정보가 비어 있음
    return range;
  }
}

std::vector<StateRange::ST_RANGE> StateRange::AND(const ST_RANGE &a, const ST_RANGE &b)
{
  std::vector<ST_RANGE> new_range;
  const ST_RANGE *small, *big;

  //a.begin 이 더 작을경우
  if (MIN(a.begin, b.begin) == 0)
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
      new_range.emplace_back(*big);
    }
    else
    {
      new_range.emplace_back(ST_RANGE{big->begin, small->end});
    }
  }
  else
  {
    //공집합
  }

  return new_range;
}

std::vector<StateRange::ST_RANGE> StateRange::OR_ARRANGE(const std::vector<ST_RANGE> &a)
{
  if (a.size() < 2)
    return a;

  // 가지고 있는 범위 데이터를 재정렬
  // 합칠수 있으면 합침
  std::vector<ST_RANGE> curr_range = a;
  std::vector<ST_RANGE> new_range;

  bool is_change = true;
  while (is_change)
  {
    new_range.clear();
    is_change = false;

    auto curr = curr_range[0];
    for (size_t i = 1; i < curr_range.size(); ++i)
    {
      auto ret = OR(curr, curr_range[i]);
      if (ret.size() > 0)
      {
        is_change = true;
        curr = ret[0];
      }
      else
      {
        new_range.emplace_back(curr);
        curr = curr_range[i];
      }
    }
    new_range.emplace_back(curr);
    curr_range = new_range;
  }

  return new_range;
}

std::vector<StateRange::ST_RANGE> StateRange::OR(const ST_RANGE &a, const ST_RANGE &b)
{
  std::vector<ST_RANGE> new_range;
  const ST_RANGE *small, *big;

  //a.begin 이 더 작을경우
  if (MIN(a.begin, b.begin) == 0)
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
    auto begin = small->begin;
    auto end = MAX(small->end, big->end) == 0 ? small->end : big->end;

    //두 범위가 겹쳐지는 구간이 있으면 합칠수 있음
    new_range.emplace_back(ST_RANGE{begin, end});
  }
  else
  {
    //두 범위가 겹쳐지는 구간이 없으면 독립적으로 존재
  }

  return new_range;
}

// a 가 작으면 : 0
// b 가 작으면 : 1
int StateRange::MIN(const StateData &a, const StateData &b)
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
int StateRange::MAX(const StateData &a, const StateData &b)
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

StateItem::StateItem()
    : condition_type(EN_CONDITION_NONE), function_type(FUNCTION_NONE)
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

  default:
    return false;
  }
}

bool StateItem::IsIntersection(const std::vector<StateRange> &a, const std::vector<StateRange> &b)
{
  if (a.size() == 0 || b.size() == 0)
  {
    return true;
  }

  size_t empty_size = 0;
  for (auto &i : a)
  {
    for (auto &j : b)
    {
      auto range = StateRange::AND(i, j);
      if (range.GetRange()->size() == 0)
      {
        ++empty_size;
      }
    }
  }

  return empty_size == a.size() * b.size() ? false : true;
}

StateRange StateItem::MakeRange()
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

StateRange StateItem::MakeRange(const std::string &column_name, bool &is_valid)
{
  RangeRecursive(*this, column_name, is_valid);
  return MakeRange(*this);
}

StateRange StateItem::MakeRange(const StateItem &item)
{
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
        auto range = StateRange::AND(MakeRange(item.arg_list[0]), MakeRange(item.arg_list[1]));

        for (size_t i = 2; i < item.arg_list.size(); ++i)
        {
          range = StateRange::AND(range, MakeRange(item.arg_list[i]));
        }

        return range;
      }
      break;

    case EN_CONDITION_OR:
      if (item.arg_list.size() > 1)
      {
        auto range = StateRange::OR(MakeRange(item.arg_list[0]), MakeRange(item.arg_list[1]));

        for (size_t i = 2; i < item.arg_list.size(); ++i)
        {
          range = StateRange::OR(range, MakeRange(item.arg_list[i]));
        }

        return range;
      }
      break;

    default:
      break;
    }

    return StateRange();
  }

  if (item.function_type != FUNCTION_NONE && is_data_ok(item) == true)
  {
    StateRange range;

    switch (item.function_type)
    {
    case FUNCTION_BETWEEN:
      range.SetBetween(item.data_list[0], item.data_list[1]);
      return range;

    case FUNCTION_EQ:
      range.SetValue(item.data_list[0], true);
      return range;

    case FUNCTION_NE:
      range.SetValue(item.data_list[0], false);
      return range;

    case FUNCTION_LT:
      range.SetEnd(item.data_list[0], false);
      return range;

    case FUNCTION_LE:
      range.SetEnd(item.data_list[0], true);
      return range;

    case FUNCTION_GT:
      range.SetBegin(item.data_list[0], false);
      return range;

    case FUNCTION_GE:
      range.SetBegin(item.data_list[0], true);
      return range;

    default:
      break;
    }

    return StateRange();
  }

  if (item.condition_type == EN_CONDITION_NONE && item.function_type == FUNCTION_NONE)
  {
    if (item.data_list.size() == 1)
    {
      StateRange range;
      range.SetValue(item.data_list[0], true);
      return range;
    }

    return StateRange();
  }

  return StateRange();
}
