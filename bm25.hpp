//
// Created by ivan on 20.12.17.
//

#ifndef FF_FAST_SEARCH_BM25_HPP
#define FF_FAST_SEARCH_BM25_HPP

#include <cstdint>
#include <utility>
#include <vector>
#include <unordered_map>
#include <cmath>


//#include <bsoncxx/builder/stream/document.hpp>
//#include <bsoncxx/json.hpp>
//
//#include <mongocxx/client.hpp>
//#include <mongocxx/instance.hpp>
//#include "json.hpp"
//using json = nlohmann::json;
//
//using bsoncxx::builder::stream::document;
//using bsoncxx::builder::stream::finalize;
//using bsoncxx::builder::stream::open_document;
//using bsoncxx::builder::stream::close_document;
//using bsoncxx::builder::stream::open_array;
//using bsoncxx::builder::stream::close_array;

namespace search {
    class BM25 {
        using uint_t = uint32_t;
        using uid_t = uint32_t;
        using small_uint_t = uint16_t;
        using long_uint_t = uint64_t;
        using user_to_num_t = std::vector<std::pair<uid_t, small_uint_t>>;

        struct info {
            using sex_t = small_uint_t;
            using city_t = small_uint_t;
            using age_t = small_uint_t;
            using relation_t = small_uint_t;

            sex_t sex_;
            city_t city_;
            age_t age_;
            relation_t relation_;
        };

        using reverse_index_t = std::unordered_map<std::string, user_to_num_t>;
        using user_length_t = std::unordered_map<uid_t, uint_t>;
        using token_freqs_t = std::unordered_map<std::string, uint_t>;
        using user_infos_t = std::unordered_map<uid_t, info>;
        using bm25_t = std::unordered_map<uid_t, double>;

    public:
        BM25() = delete;

        explicit BM25(std::string const &filename);

        std::vector<std::pair<uid_t, double>> search(std::vector<std::string> const &query,
                                                     size_t qty = 10, size_t sex = 1, size_t city = 2,
                                                     size_t age_from = 18, size_t age_to = 30, size_t relation = 6);

//        void serialize(std::string const& filename);


    private:
        void deserialize(std::string const& filename);

//        void serialize(std::ofstream *file, std::string const &str);

        void deserialize(std::ifstream *file, std::string *str, char *buffer);

//        template<class T>
//        void serialize(T const &value, std::ofstream *file);

        template<class T>
        void deserialize(T *value, std::ifstream *file);

        void update_bm25(std::string const &token, uid_t uid, size_t freq);

        void calculate_avg_length();

//        void prepare_data();
//
//        void load_user_length();
//
//        void load_token_freqs();
//
//        void load_users_infos();
//
//        void load_reverse_index();

//        mongocxx::instance instance_;
//        mongocxx::client connection_;
        reverse_index_t reverse_index_;
        user_length_t user_length_;
        token_freqs_t token_freqs_;
        user_infos_t user_infos_;
        bm25_t bm25_;

        double n_;
        double avg_length_;
        const double k1_ = 1.5;
        const double b_ = 0.75;
    };
}
#endif //FF_FAST_SEARCH_BM25_HPP
