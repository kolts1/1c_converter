
#include <iostream>
#include <fstream>
#include <string>
#include <iterator>
#include <vector>
#include <chrono>
#include <cstring>
#include <filesystem>

constexpr size_t BUFFER_SIZE = 1024 * 1024;

using namespace std::chrono;

struct type_description {
    std::vector<std::string> types;
};

struct column {
    std::string column_name;
    type_description value_type;
    int width;
};

struct table_info {
    int column_count = 0;
    long rows_count = 0;
    bool is_valid = true;
    long fileCurrentRowNumber = 0;
    enum PassedPosition{
        Closed,
        TableDefiniton,
        TableBeginBlock,
        ColumnCount,
        ColumnDefinitionStart,
        ColumnsDefinition,
        CompositeTypeDefinition,
        TypeDefinition,
        ColumnDefinitionEnd,
        ColumnsDefinitionEnd,
        ColumnsMapping,
        RowsDefinition,
        RowDefinition,
        ColumnData,
        RowDefinitionEnd,
        RowsDefinitionEnd,
        EndOfIle
    };
    PassedPosition passed_position = Closed;
    std::vector<column> columns;
    int current_column_index = 0;
    long current_row_index = -1;
    column * currentColumn;
};

void inline handleAndWriteleData(const table_info & table,char * buffer,std::string_view type,std::string_view data,size_t pos){
    //обработка данных перед записью


    //запись
    memcpy(buffer,data.data(),data.length());

}

void inline handleColumnData(std::string_view row,table_info & table,char * buffer,size_t &pos){

    auto commaIndexOf = row.find(',');
    auto bracketIndexOf = row.find('}');
    auto type = row.substr(2,1);


    auto data =
            row.substr(commaIndexOf + 1,row.length() - commaIndexOf - row.length() + bracketIndexOf - 1);

    handleAndWriteleData(table,buffer + pos, type, data, pos);

    pos += data.length();
    if(table.current_column_index < table.column_count - 1){
        buffer[pos] = '\t';
        pos++;
    } else if(table.current_row_index < table.rows_count - 1) {
        buffer[pos] = '\n';
        pos++;
    }

    table.current_column_index++;

}

std::vector<std::string_view> split_view(std::string_view str, char delim) {
    std::vector<std::string_view> result;

    size_t start = 0;
    while (start < str.size()) {
        size_t end = str.find(delim, start);
        if (end == std::string_view::npos) {
            result.emplace_back(str.substr(start));
            break;
        }
        result.emplace_back(str.substr(start, end - start));
        start = end + 1;
    }

    return result;
}

void inline handle_row(std::string_view row,table_info & table,char * buffer,size_t &pos) {
    table.fileCurrentRowNumber++;
    if (table.passed_position == table_info::Closed) {
//        if (remove_bom(row).substr(0, 41) == "{\"#\",acf6192e-81ca-46ef-93a6-5a6968b78663") {
//            table.passed_position = table_info::TableDefiniton;
//        } else {
//            throw "Неправильный тип данных";
//        }
        table.passed_position = table_info::TableDefiniton;
    } else if (table.passed_position == table_info::TableDefiniton) {

        if (row.substr(0, 3) == "{7," || row.substr(0, 3) == "{8," || row.substr(0, 3) == "{9,") {
            table.passed_position = table_info::TableBeginBlock;
        } else {
            throw "Неправильный тип данных";
        }

    } else if (table.passed_position == table_info::TableBeginBlock) {

        if (row.at(0) == '{') {
            table.column_count = std::stoi(std::string(row.substr(1, row.find(',') - 1)));
            table.passed_position = table_info::ColumnCount;
        }

    } else if (table.passed_position == table_info::ColumnCount) {

        auto t = split_view(row, ',');
        column col;
        col.column_name = std::string(t.at(1).substr(1, t.at(1).size() - 2));
        table.passed_position = table_info::ColumnDefinitionStart;
        table.columns.push_back(col);
        table.currentColumn = &table.columns.back();

    } else if (table.passed_position == table_info::ColumnDefinitionStart) {

        if (row.substr(0, 11) == "{\"Pattern\",") {
            table.passed_position = table_info::CompositeTypeDefinition;
        } else {
            table.currentColumn->value_type.types.emplace_back(row.substr(
                    row.find('{') + 1,
                    row.find('}') - 1
            ));
            table.passed_position = table_info::TypeDefinition;
        }
    } else if (table.passed_position == table_info::CompositeTypeDefinition) {
        table.currentColumn->value_type.types.emplace_back(row.substr(
                row.find('{') + 1,
                row.find('}') - 1
        ));
        table.passed_position = table_info::TypeDefinition;
    } else if (table.passed_position == table_info::TypeDefinition) {
        if (row[0] == '}') {
            table.passed_position = table_info::ColumnDefinitionEnd;
            auto t = split_view(row, ',');
            auto str = std::string(t.at(2).substr(0, t.at(2).find('}')));
            table.currentColumn->width = std::stoi(std::string(t.at(2).substr(0, t.at(2).find('}'))));
        } else { //новый тип
            table.currentColumn->value_type.types.emplace_back(row.substr(
                    row.find('{') + 1,
                    row.find('}') - 1
            ));
            table.passed_position = table_info::TypeDefinition;
        }
    } else if (table.passed_position == table_info::ColumnDefinitionEnd) {

        if (row.substr(0, 2) == "},") {
            table.passed_position = table_info::ColumnsDefinitionEnd;
        } else {
            auto t = split_view(row, ',');
            column col;
            col.column_name = std::string(t.at(1).substr(1, t.at(1).size() - 2));
            table.passed_position = table_info::ColumnDefinitionStart;
            table.columns.push_back(col);
            table.currentColumn = &table.columns.back();
        }
    } else if (table.passed_position == table_info::ColumnsDefinitionEnd) {
        table.passed_position = table_info::ColumnsMapping;

    } else if (table.passed_position == table_info::ColumnsMapping) {
        auto t = split_view(row, ',');
        table.rows_count = std::stoi(std::string(t.at(1)));
        table.passed_position = table_info::RowsDefinition;

    } else if (table.passed_position == table_info::RowsDefinition) {
        table.passed_position = table_info::RowDefinition;

    } else if (table.passed_position == table_info::RowDefinition) {
        table.current_row_index++;
        handleColumnData(row, table, buffer, pos);
        table.passed_position = table_info::ColumnData;

    } else if (table.passed_position == table_info::ColumnData) {
            handleColumnData(row, table, buffer, pos);
            if(table.current_column_index == table.column_count){
                table.current_column_index = 0;
                table.passed_position = table_info::RowDefinitionEnd;
            } else {
                table.passed_position = table_info::ColumnData;
            }
    } else if (table.passed_position == table_info::RowDefinitionEnd) {
        if (row.substr(0, 2) == "},") {
            table.passed_position = table_info::RowsDefinitionEnd;
        } else {
            table.passed_position = table_info::RowDefinition;
        }
    } else if (table.passed_position == table_info::RowsDefinitionEnd) {
        table.passed_position = table_info::EndOfIle;
    }

}



void processFile(const std::string  & filename, const std::string  & outputFilename){
    //читаем файл по 64 кбайта.
    //в вектор вставляем вью на каждую строку
    auto s1 =  std::chrono::high_resolution_clock::now();

    std::chrono::milliseconds vectorDur(0);
    std::chrono::milliseconds substrDur(0);

    table_info table;

    std::ofstream out(outputFilename);
    if (!out.is_open()) {
        std::cerr << "Unable to open out.\n";
        return;
    }

    std::ifstream in(filename, std::ios::binary);
    if (!in) {
        std::cerr << "Unable to open in.\n";
        return;
    }

    int i = 0;
    char readBuffer[BUFFER_SIZE + 1];
    char writeBuffer[BUFFER_SIZE * 5];

    while (in) {

        in.read(readBuffer, BUFFER_SIZE);
        auto dataSize = in.gcount();
        if(dataSize == BUFFER_SIZE){
            auto pos = std::string_view(readBuffer,dataSize).rfind('\n');
            if(std::string_view::npos != pos) {
                dataSize -= BUFFER_SIZE - pos;
            }
            in.seekg(-(in.gcount() - dataSize)+1, std::ios::cur);
        }
        size_t start = 0;
        auto it = 0;
        size_t writePos = 0;

        while(true){
            auto end = std::string_view(readBuffer + start,dataSize - start+1).find('\n');
            if (end == std::string_view::npos)
                break;
            end++;
            handle_row(std::string_view(readBuffer + start, end),table,writeBuffer,writePos);
            if(table.current_row_index == table.rows_count
            &&table.rows_count > 0){
                break;
            }
            if(start + 1 >= dataSize){
                break;
            }
            start += end;
            it++;
        }
        out.write(writeBuffer,writePos);
        i++;
    }
    std::cout<<"Записано "<<table.rows_count<<" строк"<<std::endl;
    std::cout<<"Общее время: "<<duration_cast<milliseconds>(high_resolution_clock::now() - s1).count() / 1000.00<<" секунд"<<std::endl;
    
}


int main(int argc,char * argv[]) {

    if(argc == 2){
        std::filesystem::path firstFile = argv[1];

        if(exists(firstFile)){
            if(is_directory(firstFile)){
                try {
                    for (const auto& entry : std::filesystem::recursive_directory_iterator(firstFile)) {
                        if (std::filesystem::is_regular_file(entry.path())) {
                            auto outFile = entry.path().parent_path() / entry.path().stem();
                            processFile(entry.path(),outFile.string() +  + ".csv");
                        }
                    }
                } catch (const std::filesystem::filesystem_error& e) {
                    std::cerr << "Ошибка: " << e.what() << '\n';
                }
            } else { //выбран 1 файл
                auto outFile = firstFile.parent_path() / firstFile.stem();
                processFile(firstFile,outFile.string() +  + ".csv");
            }
        }
    } else if(argc == 3){
        std::filesystem::path firstFile  = argv[1];
        std::filesystem::path secondFile = argv[2];

        if(std::filesystem::exists(firstFile) && std::filesystem::exists(secondFile)){
            if(std::filesystem::is_regular_file(firstFile) && std::filesystem::is_regular_file(secondFile)){
                if(std::filesystem::path(secondFile).extension() != ".csv"){
                    std::cout<<"Неправильное расширение выходного файла";
                    exit(1);
                }
                processFile(std::filesystem::path(firstFile),std::filesystem::path(secondFile));
            } else
            if(std::filesystem::is_regular_file(firstFile) && std::filesystem::is_directory(secondFile)){
                auto outFile =  (std::filesystem::path(secondFile) /
                        std::filesystem::path(firstFile).stem()).string() + ".csv" ;
                processFile(std::filesystem::path(firstFile),outFile);
            } else
            if(std::filesystem::is_directory(firstFile) && std::filesystem::is_directory(secondFile)){
                try {
                    for (const auto& entry : std::filesystem::recursive_directory_iterator(firstFile)) {
                        if (std::filesystem::is_regular_file(entry.path())) {
                            auto outFile = entry.path().parent_path() / entry.path().stem();
                            processFile(entry.path(),outFile.string() +  + ".csv");
                        }
                    }
                } catch (const std::filesystem::filesystem_error& e) {
                    std::cerr << "Ошибка: " << e.what() << '\n';
                }
            }
        }
    }

    return 0;
}
