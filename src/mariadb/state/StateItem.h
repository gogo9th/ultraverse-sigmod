#ifndef STATE_ITEM_INCLUDED
#define STATE_ITEM_INCLUDED

#include <string.h>
#include <assert.h>

#include <string>
#include <vector>
#include <algorithm>
#include <memory>

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
  
  StateData(int64_t &val);
  StateData(uint64_t &val);
  StateData(double &val);
  StateData(const std::string &val);
  
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
  
  size_t str_len;

  bool is_subselect;
  bool is_equal;
  en_state_log_column_data_type type;
  UNION_RAW_DATA d;
};



/**
 * @copilot this class represents a range of SQL where clause.
 *          for example,
 *          - "WHERE colA > 10 and colA < 20" is represented as below: (pseudo code)
 *          <code>
 *          StateRange {
 *              is_equal: false,
 *              range: [
 *                  ST_RANGE { begin: 10, end: 20 }
 *              ]
 *          }
 *          </code>
 *
 * @note 이 코드의 일부는 GitHub copilot로부터 어시스트 받아 작성하였습니다.
 */
class StateRange
{
public:
  struct ST_RANGE
  {
    StateData begin;
    StateData end;
    
    bool empty() const {
        return begin.Type() == en_column_data_null &&
               end.Type() == en_column_data_null;
    }
    
    /**
     * @copilot this function checks if two ranges are intersected.
     */
    bool isIntersection(const ST_RANGE &other) const {
        return (this->begin <= other.end) && (other.begin <= this->end);
    }
    
    [[nodiscard]]
    bool equals(const ST_RANGE &other) const {
        return this->begin == other.begin && this->end == other.end;
    }
    
    bool operator== (const ST_RANGE &other) const {
        return equals(other);
    }
    
    /**
     * @copilot this function returns the intersection of two ranges.
     */
    ST_RANGE operator& (const ST_RANGE &other) const {
        ST_RANGE range;
        if (isIntersection(other))
        {
            range.begin = std::max(this->begin, other.begin);
            range.end = std::min(this->end, other.end);
        }
        return std::move(range);
    }
    
    /**
     * @copilot this function merges two ranges into one.
     *
     * @note IsIntersection을 필요한 경우 사용하십시오
     */
    ST_RANGE operator| (const ST_RANGE &other) const {
        ST_RANGE range;
        const ST_RANGE *small, *big;
    
        //a.begin 이 더 작을경우
        if (MIN(begin, other.begin) == 0)
        {
            small = this;
            big = &other;
        }
        //b.begin 이 더 작을경우 (또는 완벽히 동일할 경우)
        else
        {
            small = &other;
            big = this;
        }
        
        if (small->begin.Type() == en_column_data_null ||
            big->begin.Type() == en_column_data_null)
        {
            range.begin = small->begin.Type() == en_column_data_null ? big->begin : small->begin;
        } else {
            range.begin = std::min(small->begin, big->begin);
        }
        
        if (small->end.Type() == en_column_data_null ||
            big->end.Type() == en_column_data_null)
        {
            range.end = small->end.Type() == en_column_data_null ? big->end : small->end;
        } else {
            range.end = std::max(small->end, big->end);
        }

        return std::move(range);
    }
    
    template <typename Archive>
    void serialize(Archive &archive);
  };

  StateRange();
  
  /** unit test를 위한 생성자 */
  StateRange(int64_t singleValue);
  /** unit test를 위한 생성자 */
  StateRange(const std::string &singleValue);
  
  ~StateRange();

  bool operator==(const StateRange &c) const;
  bool operator!=(const StateRange &other) const;
  
  /**
   * std::map에서 key로 사용하기 위한 비교 연산자
   */
  bool operator<(const StateRange &other) const;
  
  bool wildcard() const;
  void setWildcard(bool wildcard);

  std::string MakeWhereQuery();
  std::string MakeWhereQuery(std::string columnName);

  void SetBegin(const StateData &_begin, bool _add_equal);
  void SetEnd(const StateData &_end, bool _add_equal);
  void SetBetween(const StateData &_begin, const StateData &_end);
  void SetValue(const StateData &_value, bool _add_equal);
  const std::vector<ST_RANGE> *GetRange() const;
  static std::shared_ptr<std::vector<StateRange>> OR_ARRANGE(const std::vector<StateRange> &a);
  
  static bool isIntersects(const StateRange &a, const StateRange &b);
  
  void OR_FAST(const StateRange &b, bool ignoreIntersect = false);
  
  static std::shared_ptr<StateRange> AND(const StateRange &a, const StateRange &b);
  static std::shared_ptr<StateRange> OR(const StateRange &a, const StateRange &b, bool ignoreIntersect = false);

  template <typename Archive>
  void serialize(Archive &archive);

  void arrangeSelf();
  

private:
  enum EN_VALID
  {
    EN_VALID_NONE,
    EN_VALID_RANGE
  };
  static EN_VALID IsValid(const StateRange &a, const StateRange &b);
  

  static bool IsIntersection(const ST_RANGE &a, const ST_RANGE &b);

  static std::shared_ptr<std::vector<ST_RANGE>> AND(const ST_RANGE &a, const ST_RANGE &b);
  static std::shared_ptr<std::vector<ST_RANGE>> OR_ARRANGE(const std::shared_ptr<std::vector<ST_RANGE>> a);
  static std::shared_ptr<std::vector<ST_RANGE>> OR_ARRANGE2(const std::shared_ptr<std::vector<ST_RANGE>> a);
  static std::shared_ptr<std::vector<ST_RANGE>> OR(const ST_RANGE &a, const ST_RANGE &b);
  static int MIN(const StateData &a, const StateData &b);
  static int MAX(const StateData &a, const StateData &b);

  std::shared_ptr<std::vector<ST_RANGE>> range;
  bool _wildcard;
};

/**
 * @copilot this class represents expression of SQL where clause.
 *
 * @note 이 코드의 일부는 GitHub copilot로부터 어시스트 받아 작성하였습니다.
 */
class StateItem
{
public:
  StateItem();
  ~StateItem();

  /**
   * @deprecated use ::MakeRange2().
   */
  std::shared_ptr<StateRange> MakeRange();
  StateRange MakeRange2() const;
  
  /**
   * @deprecated use ::MakeRange2().
   */
  std::shared_ptr<StateRange> MakeRange(const std::string &column_name, bool &is_valid);
  static std::shared_ptr<StateRange> MakeRange(const StateItem &item);


  
  template <typename Archive>
  void serialize(Archive &archive);
private:
  static bool is_data_ok(const StateItem &item);

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
#include "StateItem.template.cpp"

#endif /* STATE_ITEM_INCLUDED */
