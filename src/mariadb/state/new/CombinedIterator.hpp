//
// Created by cheesekun on 6/20/23.
//

#ifndef ULTRAVERSE_COMBINEDITERATOR_HPP
#define ULTRAVERSE_COMBINEDITERATOR_HPP

#include <vector>
#include <functional>

namespace ultraverse {
    
    /**
     * @brief An iterator that iterates over multiple std::vector<>s.
     */
    template<typename Value>
    class CombinedIterator {
    public:
        typedef CombinedIterator<Value> Iterator;
        
        typedef std::ptrdiff_t difference_type;
        typedef Value value_type;
        typedef Value *pointer;
        typedef Value &reference;
        typedef size_t size_type;
        typedef std::forward_iterator_tag iterator_category;
        
        using Container = std::vector<Value>;
        using ContainerHolder = std::vector<std::reference_wrapper<Container>>;
        
        
        explicit CombinedIterator(ContainerHolder &containers):
            _containers(containers),
            _containerIndex(0),
            _elemIndex(0)
        {
            sanityCheck();
        }
        
        const Value &operator*() const {
            return _containers[_containerIndex].get()[_elemIndex];
        }
        
        CombinedIterator &operator++() {
            if (_elemIndex < (int) _containers[_containerIndex].get().size() - 1) {
                _elemIndex++;
            } else {
                _containerIndex++;
                _elemIndex = 0;
            }
            
            sanityCheck();
            
            return *this;
        }
        
        bool operator==(const CombinedIterator &other) const {
            return _containerIndex == other._containerIndex && _elemIndex == other._elemIndex;
        }
        
        bool operator!=(const CombinedIterator &other) const {
            return !(this->operator==(other));
        }
        
        CombinedIterator<Value> end() const {
            CombinedIterator<Value> it = *this;
            it._containerIndex = _containers.size();
            it._elemIndex = 0;
            
            return std::move(it);
        }
        
    private:
        void sanityCheck() {
            if (_containerIndex < _containers.size() && _containers[_containerIndex].get().empty()) {
                ++(*this);
            }
        }
        
        ContainerHolder _containers;
        
        int _containerIndex;
        int _elemIndex;
    };
}


#endif //ULTRAVERSE_COMBINEDITERATOR_HPP
