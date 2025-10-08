from typing import Optional

class Reformatter:

    @staticmethod
    def decode(file_path: str, decoding: str) -> Optional[str]:
        with open(file_path, "rb") as f:
            content = f.read()
        
        errors_handling = "replace"
        decoded_content = content.decode(decoding, errors_handling)
        return decoded_content
            
    @staticmethod
    def encoding_to_target_encoding(file_path: str, source_encoding: str, temp_path: str, target_encoding: str):
        with open(file_path, "rb") as f:
            content = f.read()

        # 使用strict模式，如果有解码错误就抛出异常
        try:
            decoded_content = content.decode(source_encoding, errors="strict")
        except UnicodeDecodeError as e:
            raise Exception(f"文件 {file_path} 包含無法用 {source_encoding} 解碼的內容: {e}")

        with open(temp_path, "w", encoding=target_encoding) as f:
            f.write(decoded_content)

    @staticmethod
    def insert_delimiter_with_sizes_file(file_path: str, col_size_file: str, temp_path: str, delimiter: str):
        with open(file_path, "rb") as f:
            content_bytes = f.read()

        if '\\u' in delimiter or '\\t' in delimiter:
            delimiter = delimiter.encode().decode('unicode_escape').encode("utf-8")
        else:
            delimiter = delimiter.encode("utf-8")
        
        col_sizes = Reformatter._read_sizes_file(col_size_file, encoding = "utf-8")

        #將原始檔案內容分行寫進列表
        lines = Reformatter._split_content_with_row_lists(content_bytes)

        output = []
        for line, nl in lines:
            reformated_line = Reformatter._split_line_by_col_size(line, col_sizes)
            output.append(delimiter.join(reformated_line) + nl)

        result = b"".join(output)
        with open(temp_path, "wb") as f:
            reformated_file =f.write(result)
        
        return reformated_file

    @staticmethod
    def remove_header(file_path: str, temp_path: str, encoding: str):
        with open(file_path, "r", encoding = encoding) as f:
            lines = f.readlines()
        data_lines = lines[1:]
        result = "".join(data_lines)

        with open(temp_path, "w", encoding = encoding) as f:
            f.write(result)
    
    @staticmethod
    def _read_sizes_file(col_size_file: str, encoding: str):
        col_sizes = []
        col_sizes_str = Reformatter.decode(col_size_file, encoding)
        if not col_sizes_str:
            raise Exception("長度檔為空")

        for raw in col_sizes_str.splitlines():
            space = raw.strip()
            if not space:
                continue
            size = space.rstrip(",").strip()
            try:
                col_sizes.append(int(size))
            except ValueError:
                raise Exception(f"長度檔 {col_size_file} 的欄位長度不是數字: {size}")
        return col_sizes

    @staticmethod
    def _split_line_by_col_size(line: bytes, col_sizes: list[int]) -> list[bytes]:
        #針對一筆資料進行分隔
        fields, pos = [], 0
        for col_size in col_sizes:
            fields.append(line[pos:pos+col_size])
            pos += col_size
        return fields

    @staticmethod
    def _split_content_with_row_lists(content):
        if isinstance(content, str):
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
            return lines
        elif isinstance(content, bytes):
            lines = []
            for raw in content.splitlines(keepends=True):
                if raw.endswith(b"\r\n"):
                    line, nl = raw[:-2], b"\r\n"
                elif raw.endswith(b"\n"):
                    line, nl = raw[:-1], b"\n"
                elif raw.endswith(b"\r"):
                    line, nl = raw[:-1], b"\r"
                else:
                    line, nl = raw, b""
                lines.append((line, nl))
            return lines

if __name__ == "__main__":

    # 測試 decode
    text = Reformatter.decode("/Users/rover/ftp-root/incoming/aaa1_20250811.CSV", "utf-8")
    print(f"Decoded Content:\n{text}")

    # # 測試置入分隔符號
    # result = Reformatter.insert_delimiter_with_fixed_size("/Users/rover/ftp-root/incoming/aaa1_20250826.txt", 12, "|", "utf-8")
    # print(f"置換固定分隔符號結果:\n{result}")


    result_non_fixed = Reformatter.insert_delimiter_with_sizes_file("/Users/rover/ftp-root/incoming/aaa1_20250811.csv", "/Users/rover/Desktop/work/reformate/aaa2_20250826_reformate.txt", "|", "utf-8")
    print(f"置換固定分隔符號結果:\n{result_non_fixed}")
