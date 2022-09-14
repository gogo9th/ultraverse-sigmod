#ifndef STATE_ITEM_INCLUDED
#define STATE_ITEM_INCLUDED

#include <string.h>
#include <assert.h>

#include <string>
#include <vector>
#include <algorithm>

#include "state_log_hdr.h"

enum EN_CONDITION_TYPE
{
  EN_CONDITION_NONE = 0,
  EN_CONDITION_AND,
  EN_CONDITION_OR
};

enum EN_FUNCTION_TYPE
{
  FUNCTION_NONE = 0,
  FUNCTION_EQ,
  FUNCTION_NE,
  FUNCTION_LT,
  FUNCTION_LE,
  FUNCTION_GT,
  FUNCTION_GE,
  FUNCTION_BETWEEN,
};

class StateData
{
public:
  StateData();
  StateData(const StateData &c);
  ~StateData();

  bool SetData(en_state_log_column_data_type _type, void *_data, size_t _length);
  bool ConvertData(en_state_log_column_data_type _type);

  void SetEqual();
  bool IsEqual() const;
  bool IsNone() const;
  bool IsSubSelect() const;
  en_state_log_column_data_type Type() const;

  void Set(int64_t val);
  void Set(uint64_t val);
  void Set(double val);
  void Set(const char *val, size_t length);

  bool Get(int64_t &val) const;
  bool Get(uint64_t &val) const;
  bool Get(double &val) const;
  bool Get(std::string &val) const;

  bool operator==(const StateData &c) const;
  bool operator!=(const StateData &c) const;
  bool operator>(const StateData &c) const;
  bool operator>=(const StateData &c) const;
  bool operator<(const StateData &c) const;
  bool operator<=(const StateData &c) const;
  StateData &operator=(const StateData &c);
  
  template <typename Archive>
  void save(Archive &archive) const;
  
  template <typename Archive>
  void load(Archive &archive);

private:
  void Clear();
  void Copy(const StateData &c);

  union UNION_RAW_DATA {
    int64_t ival;
    uint64_t uval;
    double fval;
    char *str;
  };

  bool is_subselect;
  bool is_equal;
  en_state_log_column_data_type type;
  UNION_RAW_DATA d;
};

class StateRange
{
public:
  struct ST_RANGE
  {
    StateData begin;
    StateData end;
    
    template <typename Archive>
    void serialize(Archive &archive);
  };

  StateRange();
  ~StateRange();

  bool operator==(const StateRange &c) const;

  std::string MakeWhereQuery();

  void SetBegin(const StateData &_begin, bool _add_equal);
  void SetEnd(const StateData &_end, bool _add_equal);
  void SetBetween(const StateData &_begin, const StateData &_end);
  void SetValue(const StateData &_value, bool _add_equal);
  const std::vector<ST_RANGE> *GetRange() const;
  static std::vector<StateRange> OR_ARRANGE(const std::vector<StateRange> &a);

  static StateRange AND(const StateRange &a, const StateRange &b);
  static StateRange OR(const StateRange &a, const StateRange &b);

  template <typename Archive>
  void serialize(Archive &archive);

private:
  enum EN_VALID
  {
    EN_VALID_NONE,
    EN_VALID_RANGE
  };
  static EN_VALID IsValid(const StateRange &a, const StateRange &b);
  static bool IsIntersection(const ST_RANGE &small, const ST_RANGE &big);

  static std::vector<ST_RANGE> AND(const ST_RANGE &a, const ST_RANGE &b);
  static std::vector<ST_RANGE> OR_ARRANGE(const std::vector<ST_RANGE> &a);
  static std::vector<ST_RANGE> OR(const ST_RANGE &a, const ST_RANGE &b);
  static int MIN(const StateData &a, const StateData &b);
  static int MAX(const StateData &a, const StateData &b);

  std::vector<ST_RANGE> range;
};

class StateItem
{
public:
  StateItem();
  ~StateItem();

  StateRange MakeRange();
  StateRange MakeRange(const std::string &column_name, bool &is_valid);

  // 두 범위(또는 값)가 교집합인지 공집합인지 확인
  // 정보가 불완전 할 경우 / 기본값은 교집합으로 함
  // 공집합인 경우 쿼리간 의존관계가 없어지기 때문에 데이터가 모두 유효할때만 신중하게 결정
  // 교집합이면 true, 공집합이면 false
  static bool IsIntersection(const std::vector<StateRange> &a, const std::vector<StateRange> &b);
  
  template <typename Archive>
  void serialize(Archive &archive);
private:
  static bool is_data_ok(const StateItem &item);
  static StateRange MakeRange(const StateItem &item);

public:
  EN_CONDITION_TYPE condition_type;
  EN_FUNCTION_TYPE function_type;
  std::string name;
  // lvalue
  std::vector<StateItem> arg_list;
  // rvalue
  std::vector<StateData> data_list;
  std::vector<StateItem> sub_query_list;
};

#include "StateItem.cereal.cpp"

#endif /* STATE_ITEM_INCLUDED */
