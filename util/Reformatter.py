from cgitb import reset
from curses import endwin
from typing import Optional

class Reformatter:

    @staticmethod
    def decode(file_path: str, preferred_encoding: str) -> Optional[str]:
        with open(file_path, "rb") as f:
            content = f.read()
        
        errors_handling = "replace"
        decoded_content = content.decode(preferred_encoding, errors_handling)
        if decoded_content is not None:
            return decoded_content

        if decoded_content is None:
            print(f" {file_path} 解碼失敗，無法用指定的 {preferred_encoding} 模式進行解碼")
            return None
    
    @staticmethod
    def encoding_to_uft_8(file_path: str, encoding: str, temp_upload_path: str):
        content = Reformatter.decode(file_path, encoding)
        with open(temp_upload_path, "w", encoding="utf-8") as f:
            f.write(content)


    @staticmethod
    def _read_sizes_file(col_size_file: str, encoding: str):
        col_sizes = []
        col_sizes_str = Reformatter.decode(col_size_file, encoding)
        if col_sizes_str is None:
            raise ValueError("解碼失敗")

        for raw in col_sizes_str.splitlines():
            space = raw.strip()
            if not space:
                continue
            size = space.rstrip(",").strip()
            col_sizes.append(int(size))
        if not col_sizes:
            raise ValueError("長度檔為空")

        return col_sizes

    @staticmethod
    def _split_line_by_col_size(line: str, col_sizes: list[int]):
        #針對一筆資料進行分隔
        fields, pos = [], 0
        for col_size in col_sizes:
            fields.append(line[pos:pos+col_size])
            pos += col_size
        return fields

    @staticmethod
    def _split_content_with_row_lists(content: str):
        lines = []
        for raw in content.splitlines(keepends=True):
            if raw.endswith("\r\n"):
                line, nl = raw[:-2], "\r\n"
            elif raw.endswith("\n"):
                line, nl = raw[:-1], "\n"
            elif raw.endswith("\r"):
                line, nl = raw[:-1], "\r"
            else:
                line, nl = raw, ""
            lines.append((line, nl))
        # print(f"讀取換行陣列和換行符號：\n{lines}")
        return lines

    @staticmethod
    def insert_delimiter_with_sizes_file(file_path: str, col_size_file: str, temp_upload_path: str):
        content = Reformatter.decode(file_path,"utf-8")
        delimiter = ","
        if content is None:
            raise ValueError("解碼失敗")


        col_sizes = Reformatter._read_sizes_file(col_size_file, encoding = "utf-8")
        lines = Reformatter._split_content_with_row_lists(content)

        output = []
        for line, nl in lines:
            reformated_line = Reformatter._split_line_by_col_size(line, col_sizes)
            output.append(delimiter.join(reformated_line) + nl)

        result = "".join(output)
        with open(temp_upload_path, "w", encoding="utf-8") as f:
            f.write(result) 
 
    @staticmethod
    def remove_header(file_path: str, temp_upload_path: str):
        with open(file_path, "r", encoding = "utf-8") as f:
            lines = f.readlines()
        data_lines = lines[1:]
        result = "".join(data_lines)

        with open(temp_upload_path, "w", encoding = "utf-8") as f:
            f.write(result)



    @staticmethod
    def reformat(file_path, output_path, delimiter=",", new_line_ch="\n", encoding="utf-8"):
        with open(file_path, "r", encoding=encoding) as f:
            lines = f.readlines()
        with open(output_path, "w", encoding=encoding) as f:
            for line in lines:
                f.write(line)


if __name__ == "__main__":

    # 測試 decode
    text = Reformatter.decode("/Users/rover/ftp-root/incoming/aaa1_20250811.CSV", "utf-8")
    print(f"Decoded Content:\n{text}")

    # # 測試置入分隔符號
    # result = Reformatter.insert_delimiter_with_fixed_size("/Users/rover/ftp-root/incoming/aaa1_20250826.txt", 12, "|", "utf-8")
    # print(f"置換固定分隔符號結果:\n{result}")


    result_non_fixed = Reformatter.insert_delimiter_with_sizes_file("/Users/rover/ftp-root/incoming/aaa1_20250811.csv", "/Users/rover/Desktop/work/reformate/aaa2_20250826_reformate.txt", "|", "utf-8")
    print(f"置換固定分隔符號結果:\n{result_non_fixed}")
