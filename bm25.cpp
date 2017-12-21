//
// Created by ivan on 21.12.17.
//
#include "bm25.hpp"
#include <iostream>
#include <fstream>
#include <algorithm>

namespace search {

   /* BM25::BM25() : instance_{}, connection_{mongocxx::uri{}} {
        prepare_data();
        n_ = user_length_.size();
        calculate_avg_length();
    }

    BM25::BM25(std::string const &filename)
            : instance_{}, connection_{mongocxx::uri{}} {
        deserialize(filename);
        n_ = user_length_.size();
        calculate_avg_length();
    }*/


    BM25::BM25(std::string const &filename)
    {
        deserialize(filename);
        n_ = user_length_.size();
        calculate_avg_length();
    }


    std::vector <std::pair<uid_t, double>> BM25::search(std::vector <std::string> const &query,
                                                        size_t qty, size_t sex, size_t city,
                                                        size_t age_from, size_t age_to, size_t relation) {
 /*               auto array_builder = bsoncxx::builder::basic::array{};
                for (const auto& token : query) {
                    array_builder.append(token);
                }
                auto collection = connection_["ir_project"]["reverse_index"];
                auto cursor = collection.find(document{}
                                                      << "token" << open_document << "$in"
                                                      << array_builder << close_document << finalize);
                for (auto& doc : cursor) {
                    json cur_doc = json::parse(bsoncxx::to_json(doc));

                    for (auto& pairs: cur_doc["uids_freqs"]){
                        uid_t uid = pairs.at(0);
                        size_t freq = pairs.at(1);
                        update_bm25(cur_doc["token"], uid, freq);
                    }
                }*/

        for (auto &token: query) {
            for (auto &pairs: reverse_index_[token]) {
                uid_t uid = pairs.first;
                size_t freq = pairs.second;
                update_bm25(token, uid, freq);
            }
        }

        std::vector <std::pair<uid_t, double>> uid_score;

        for (auto &pair: bm25_) {
            uid_t uid = pair.first;
            info user_info = user_infos_[uid];

            if (user_info.sex_ != sex
                or user_info.city_ != city
                or user_info.age_ < age_from
                or user_info.age_ > age_to
                or user_info.relation_ != relation) {
                continue;
            }
            uid_score.emplace_back(uid, pair.second);
        }

        std::partial_sort(uid_score.begin(), uid_score.begin() + qty, uid_score.end(),
                          [](std::pair<int, double> const &left,
                             std::pair<int, double> const &right) {
                              return left.second > right.second;
                          });

        std::vector <std::pair<uid_t, double>> result(qty);
        std::copy(uid_score.begin(), uid_score.begin() + qty, result.begin());

        bm25_.clear();

        return result;
    }

  /*  void BM25::serialize(std::string const& filename) {
        std::ofstream output_file(filename, std::ios::binary);
        size_t size;

        size = reverse_index_.size();
        serialize(size, &output_file);

        for (auto &pairs: reverse_index_) {
            serialize(&output_file, pairs.first);
            size_t local_size = pairs.second.size();
            serialize(local_size, &output_file);

            for (auto &pair: pairs.second) {
                serialize(pair.first, &output_file);
                serialize(pair.second, &output_file);
            }
        }

        size = user_length_.size();
        serialize(size, &output_file);
        for (auto &item: user_length_) {
            serialize(item.first, &output_file);
            serialize(item.second, &output_file);
        }

        size = token_freqs_.size();
        serialize(size, &output_file);
        for (auto &item: token_freqs_) {
            serialize(&output_file, item.first);
            serialize(item.second, &output_file);
        }

        size = user_infos_.size();
        serialize(size, &output_file);
        for (auto &item: user_infos_) {
            serialize(item.first, &output_file);
            serialize(item.second.sex_, &output_file);
            serialize(item.second.city_, &output_file);
            serialize(item.second.age_, &output_file);
            serialize(item.second.relation_, &output_file);
        }
    }*/

    void BM25::deserialize(std::string const& filename) {
        std::ifstream input_file(filename, std::ios::binary);
        char buffer[100000];
        std::string str_buffer;
        size_t size;
        size_t local_size;

        deserialize(&size, &input_file);

        for (size_t i = 0; i < size; ++i) {
            deserialize(&input_file, &str_buffer, buffer);
            deserialize(&local_size, &input_file);
            for (size_t j = 0; j < local_size; ++j) {
                typename reverse_index_t::mapped_type::value_type::first_type uid;
                typename reverse_index_t::mapped_type::value_type::second_type freq;
                deserialize(&uid, &input_file);
                deserialize(&freq, &input_file);
                reverse_index_[str_buffer].emplace_back(uid, freq);
            }
        }

        deserialize(&size, &input_file);
        for (size_t i = 0; i < size; ++i) {
            typename user_length_t::key_type uid;
            typename user_length_t::mapped_type length;
            deserialize(&uid, &input_file);
            deserialize(&length, &input_file);
            user_length_[uid] = length;
        }

        deserialize(&size, &input_file);
        for (size_t i = 0; i < size; ++i) {
            typename token_freqs_t::mapped_type freq;
            deserialize(&input_file, &str_buffer, buffer);
            deserialize(&freq, &input_file);
            token_freqs_[str_buffer] = freq;
        }

        deserialize(&size, &input_file);
        for (size_t i = 0; i < size; ++i) {
            typename user_infos_t::key_type uid;
            typename user_infos_t::mapped_type::sex_t sex;
            typename user_infos_t::mapped_type::city_t city;
            typename user_infos_t::mapped_type::age_t age;
            typename user_infos_t::mapped_type::relation_t relation;
            deserialize(&uid, &input_file);
            deserialize(&sex, &input_file);
            deserialize(&city, &input_file);
            deserialize(&age, &input_file);
            deserialize(&relation, &input_file);
            user_infos_[uid] = {sex, city, age, relation};
        }
    }

/*    void BM25::serialize(std::ofstream *file, std::string const& str) {
        size_t str_size = str.size();
        file->write(reinterpret_cast<const char *>(&str_size), sizeof(str_size));
        file->write(str.c_str(), str_size);
    }*/

    void BM25::deserialize(std::ifstream* file, std::string* str, char* buffer) {
        size_t str_size;
        file->read(reinterpret_cast<char *>(&str_size), sizeof(str_size));
        file->read(buffer, str_size);
        buffer[str_size] = '\0';
        *str = std::string(buffer);
    }

 /*   template<class T>
    void BM25::serialize(T const &value, std::ofstream *file) {
        file->write(reinterpret_cast<const char *>(&value), sizeof(value));
    }*/

    template<class T>
    void BM25::deserialize(T *value, std::ifstream *file) {
        file->read(reinterpret_cast<char *>(value), sizeof(*value));
    }

    void BM25::update_bm25(std::string const &token, uid_t uid, size_t freq) {
        double update = log(n_ / token_freqs_[token]) * ((k1_ + 1) * freq) /
                        (k1_ * ((1 - b_) + b_ * (user_length_[uid] / avg_length_)) + freq);

        bm25_[uid] += update;
    }

    void BM25::calculate_avg_length() {
        double avg = 0;
        for (auto &item: user_length_) {
            avg += static_cast<double>(item.second);
        }
        avg_length_ = avg / n_;
    }

/*    void BM25::prepare_data() {
        load_reverse_index();
        load_user_length();
        load_token_freqs();
        load_users_infos();
    }

    void BM25::load_user_length() {
        std::cout << "loading user length" << std::endl;
        auto collection = connection_["ir_project"]["user_length"];
        auto cursor = collection.find({});
        for (auto &&doc : cursor) {
            json cur_doc = json::parse(bsoncxx::to_json(doc));
            user_length_[cur_doc["uid"]] = cur_doc["length"];
        }
    }

    void BM25::load_token_freqs() {
        std::cout << "loading token freqs" << std::endl;
        auto collection = connection_["ir_project"]["token_freqs"];
        auto cursor = collection.find({});
        for (auto &&doc : cursor) {
            json cur_doc = json::parse(bsoncxx::to_json(doc));
            token_freqs_[cur_doc["token"]] = cur_doc["freq"];
        }
    }

    void BM25::load_users_infos() {
        std::cout << "loading users infos" << std::endl;
        auto collection = connection_["ir_project"]["users_infos"];
        auto cursor = collection.find({});
        for (auto &&doc : cursor) {
            json cur_doc = json::parse(bsoncxx::to_json(doc));
            user_infos_[cur_doc["uid"]] = info{cur_doc["sex"], cur_doc["city"], cur_doc["age"], cur_doc["relation"]};
        }
    }

    void BM25::load_reverse_index() {
        std::cout << "loading reverse index" << std::endl;
        auto collection = connection_["ir_project"]["reverse_index"];
        auto cursor = collection.find({});
        for (auto &doc : cursor) {
            json cur_doc = json::parse(bsoncxx::to_json(doc));
            for (auto &pairs: cur_doc["uids_freqs"]) {
                uid_t uid = pairs.at(0);
                size_t freq = pairs.at(1);
                reverse_index_[cur_doc["token"]].emplace_back(uid, freq);
            }
        }
    }*/
}