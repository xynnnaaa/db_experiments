#include <bits/stdc++.h>
using namespace std;

bool isOnlyDigits(const string& str) {
    for (char const &c : str) {
        if (!isdigit(c) && !isspace(c)) // 检查是否只包含数字和空格
            return false;
    }
    return true;
}

int main() {

    // std::string file_name = "train_";

    for(int i=0; i<8; i++){
        // 打开文件进行写入
        // std::ofstream outfile("test/test_union.bitmaps", std::ios::binary);
        // std::ifstream inputFile("test/test_sample_union.txt");
        // std::ifstream numFile("test/test_num.txt");

        std::string file_name = "test/test_" + std::to_string(i);

        // std::string file_name = "train/train_" + std::to_string(i);

        std::ofstream outfile(file_name + ".bitmaps", std::ios::binary);
        std::ifstream inputFile(file_name + "_sample.txt");
        std::ifstream numFile(file_name + "_num.txt");//存储每个查询表格的数量

        std::vector<int> number_list; // 示例列表
        const int size = 1000; // 比特位的数量
        const int bytes_needed = (size + 7) / 8; // 计算所需的字节数

        std::vector<char> bit_vector(bytes_needed, 0); // 初始化字节向量

        int num_bitmaps_curr_query;

        std::string line;
        std::string line2;

        while(getline(numFile, line2)) {
            num_bitmaps_curr_query = std::stoi(line2);//获取当前query的bitmap数量
            int i=0;
            outfile.write(reinterpret_cast<const char*>(&num_bitmaps_curr_query), sizeof(num_bitmaps_curr_query));
            while(i<num_bitmaps_curr_query){
                while(getline(inputFile, line)){
                    if(isOnlyDigits(line)) {
                        number_list.push_back(std::stoi(line));
                        continue;
                    }
                    if(line.find("row") != std::string::npos) {//一个小的query结束
                        break;
                    }
                }
                // 将列表中的数字对应的比特位置为1
                for (int num : number_list) {
                    if (num > 0 && num <= size) {
                        int byte_index = (num - 1) / 8;
                        int bit_index = (num - 1) % 8;
                        bit_vector[byte_index] |= (1 << bit_index);
                    }
                }
                if (outfile.is_open()) {
                    outfile.write(bit_vector.data(), bit_vector.size());
                    //outfile.close();
                } else {
                    std::cerr << "无法打开文件进行写入!" << std::endl;
                }

                number_list.clear();
                bit_vector.assign(bytes_needed, 0);
                i++;
            }
        }

        outfile.close();
        inputFile.close();
        numFile.close();
    }

    return 0;
}