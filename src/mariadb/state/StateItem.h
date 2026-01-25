#ifndef STATE_ITEM_INCLUDED
#define STATE_ITEM_INCLUDED

#include <string.h>
#include <assert.h>

#include <string>
#include <vector>
#include <algorithm>
#include <memory>

#include "mariadb/state/new/proto/ultraverse_state_fwd.hpp"

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
  FUNCTION_IN_INTERNAL,
  FUNCTION_WILDCARD
};

class StateData
{
public:
  StateData();
  
  StateData(int64_t val);
  StateData(uint64_t val);
  StateData(double val);
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
  void SetDecimal(const std::string &val);
  void SetDecimal(const char *val, size_t length);

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
  
  void calculateHash();
  std::size_t hash() const;
  
  template <typename Archive>
  void save(Archive &archive) const;
  
  template <typename Archive>
  void load(Archive &archive);

  void toProtobuf(ultraverse::state::v2::proto::StateData *out) const;
  void fromProtobuf(const ultraverse::state::v2::proto::StateData &msg);
  
  template <typename T>
  T getAs() const {
      T val;
      Get(val);
      
      return val;
  }
  
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
  
  std::size_t _hash;
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
        // Treat NULL as unbounded on that side.
        if (!this->end.IsNone() && !other.begin.IsNone() &&
            this->end.Type() != other.begin.Type()) {
            return false;
        }
        if (!this->end.IsNone() && !other.begin.IsNone()) {
            if (this->end < other.begin) {
                return false;
            }
            if (this->end == other.begin && !(this->end.IsEqual() && other.begin.IsEqual())) {
                return false;
            }
        }
        if (!other.end.IsNone() && !this->begin.IsNone() &&
            other.end.Type() != this->begin.Type()) {
            return false;
        }
        if (!other.end.IsNone() && !this->begin.IsNone()) {
            if (other.end < this->begin) {
                return false;
            }
            if (other.end == this->begin && !(other.end.IsEqual() && this->begin.IsEqual())) {
                return false;
            }
        }
        return true;
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
        if (!isIntersection(other)) {
            return range;
        }

        auto pick_begin = [](const StateData &a, const StateData &b) -> const StateData& {
            if (a.IsNone()) {
                return b;
            }
            if (b.IsNone()) {
                return a;
            }
            if (a < b) {
                return b;
            }
            if (b < a) {
                return a;
            }
            if (!a.IsEqual()) {
                return a;
            }
            if (!b.IsEqual()) {
                return b;
            }
            return a;
        };

        auto pick_end = [](const StateData &a, const StateData &b) -> const StateData& {
            if (a.IsNone()) {
                return b;
            }
            if (b.IsNone()) {
                return a;
            }
            if (a < b) {
                return a;
            }
            if (b < a) {
                return b;
            }
            if (!a.IsEqual()) {
                return a;
            }
            if (!b.IsEqual()) {
                return b;
            }
            return a;
        };

        range.begin = pick_begin(this->begin, other.begin);
        range.end = pick_end(this->end, other.end);
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
        if (Min(begin, other.begin) == 0)
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

    void toProtobuf(ultraverse::state::v2::proto::StateRangeInterval *out) const;
    void fromProtobuf(const ultraverse::state::v2::proto::StateRangeInterval &msg);
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
  std::string MakeWhereQuery(std::string columnName) const;

  void SetBegin(const StateData &_begin, bool _add_equal);
  void SetEnd(const StateData &_end, bool _add_equal);
  void SetBetween(const StateData &_begin, const StateData &_end);
  void SetValue(const StateData &_value, bool _add_equal);
  void SetValues(const StateData &_values);
  
  const std::vector<ST_RANGE> *GetRange() const;
  static std::shared_ptr<std::vector<StateRange>> OR_ARRANGE(const std::vector<StateRange> &a);
  
  static bool isIntersects(const StateRange &a, const StateRange &b);
  
  void OR_FAST(const StateRange &b, bool ignoreIntersect = false);
  
  static std::shared_ptr<StateRange> AND(const StateRange &a, const StateRange &b);
  static std::shared_ptr<StateRange> OR(const StateRange &a, const StateRange &b, bool ignoreIntersect = false);

  template <typename Archive>
  void serialize(Archive &archive);

  void toProtobuf(ultraverse::state::v2::proto::StateRange *out) const;
  void fromProtobuf(const ultraverse::state::v2::proto::StateRange &msg);

  void arrangeSelf();
  
  void calculateHash();
  std::size_t hash() const;

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
  static int Min(const StateData &a, const StateData &b);
  static int Max(const StateData &a, const StateData &b);

  void ensureUniqueRange();

  std::shared_ptr<std::vector<ST_RANGE>> range;
  bool _wildcard;
  
  std::size_t _hash;
};

/**
 * this class represents expression of SQL where clause.
 * and, this class also represents a single key-value (range) pair (eg. user.id = 1 OR 2)
 *
 *
 * @note 이 코드의 일부는 GitHub copilot로부터 어시스트 받아 작성하였습니다.
 */
class StateItem
{
public:
  StateItem();
  StateItem(const StateItem &other);
  ~StateItem();
  
  static StateItem EQ(const std::string &name, const StateData &data);
  static StateItem Wildcard(const std::string &name);

  /**
   * @deprecated use ::MakeRange2().
   */
  std::shared_ptr<StateRange> MakeRange();
  const StateRange &MakeRange2() const;
  
  /**
   * @deprecated use ::MakeRange2().
   */
  std::shared_ptr<StateRange> MakeRange(const std::string &column_name, bool &is_valid);
  static std::shared_ptr<StateRange> MakeRange(const StateItem &item);


  
  template <typename Archive>
  void serialize(Archive &archive);

  void toProtobuf(ultraverse::state::v2::proto::StateItem *out) const;
  void fromProtobuf(const ultraverse::state::v2::proto::StateItem &msg);
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
  
  mutable StateRange _rangeCache;
  mutable bool _isRangeCacheBuilt;
};

#include "StateItem.template.cpp"

#endif /* STATE_ITEM_INCLUDED */
