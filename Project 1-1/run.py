from lark import Lark, Transformer
import sys

with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

class MyTransformer(Transformer):       # Lark의 Transformer를 상속받아 각 query에 해당하는 내용을 출력
    def create_table_query(self, items):
        print(prompt + '\'CREATE TABLE\' requested')
    def drop_table_query(self, items):
        print(prompt + '\'DROP TABLE\' requested')
    def explain_query(self, items):
        print(prompt + '\'EXPLAIN\' requested')
    def describe_query(self, items):
        print(prompt + '\'DESCRIBE\' requested')
    def desc_query(self, items):
        print(prompt + '\'DESC\' requested')
    def insert_query(self, items):
        print(prompt + '\'INSERT\' requested')
    def delete_query(self, items):
        print(prompt + '\'DELETE\' requested')
    def select_query(self, items):
        print(prompt + '\'SELECT\' requested')
    def update_query(self, items):
        print(prompt + '\'UPDATE\' requested')
    def show_tables_query(self, items):
        print(prompt + '\'SHOW TABLES\' requested')
    def EXIT(self, items):
        exit()
    
prompt = "DB_2020-11782> "  # 학번이 들어간 프롬프트

while True: # exit;가 입력되기 전까지 반복
    query = input(prompt).strip()   # query를 받아오기 위해 프롬프트를 출력하는 동시에 입력 대기, strip 함수로 \n과 같은 개행 문자 제거
    if not query: # query가 비어있으면 새로 query 입력 대기
        continue
    if query[-1] != ';':    # enter key가 입력됐지만 그 끝에 ;가 없는 경우 추가적으로 입력을 받음
        input_line = ""
        while True: # 세미콜론이 오기 전까지 input_line에 입력을 받고, 받는 동안 프롬프트를 다시 출력하지 않음
            input_line = sys.stdin.readline().strip()
            query += " " + input_line
            if not input_line: # 입력이 비어있으면 새로 query 입력 대기
                continue
            if input_line[-1] == ";":   # 세미콜론이 오면 입력 종료
                break
    query_list = query.split(";")[:-1]  # query가 여러 개 동시에 입력될 수 있으므로 ;에 따라 query를 분리해 리스트로 저장, 마지막 원소는 ""이므로 제외
    for q in query_list: #각 query에 대해 sql parse 실행
        try:
            output = sql_parser.parse(q+';') # 리스트에 저장된 query는 ;가 제거된 상태이므로 다시 추가해 parse 실행
        except Exception as e:
            print(prompt+"Syntax error") # exception 발생시 Syntax error를 출력하고 해당 쿼리 입력에 대한 출력을 마침
            break
        MyTransformer().transform(output)   # parse가 성공적으로 이루어지면, 앞서 구현한 transformer를 이용해 입력된 query의 종류 출력