from lark import Lark, Transformer
from berkeleydb import db
import sys, os
import json

with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

class Messages():
    def SyntaxError():
        print(prompt+"Syntax error")
    def CreateTableSuccess(tableName):
        print(prompt+ f"\'{tableName}\'" + " table is created")
    def DuplicateColumnDefError():
        print(prompt+"Create table has failed: column definition is duplicated")
    def DuplicatePrimaryKeyDefError():
        print(prompt+"Create table has failed: primary key definition is duplicated")
    def ReferenceTypeError():
        print(prompt+"Create table has failed: foreign key references wrong type")
    def ReferenceNonPrimaryKeyError():
        print(prompt+"Create table has failed: foreign key references non primary key column")
    def ReferenceColumnExistenceError():
        print(prompt+"Create table has failed: foreign key references non existing column")
    def ReferenceTableExistenceError():
        print(prompt+"Create table has failed: foreign key references non existing table")
    def NonExistingColumnDefError(colName):
        print(prompt+"Create table has failed: " + f"\'{colName}\'" + " does not exist in column definition")
    def TableExistenceError():
        print(prompt+"Create table has failed: table with the same name already exists")
    def CharLengthError():
        print(prompt+"Char length should be over 0")
    def DropSuccess(tableName):
        print(prompt+ f"\'{tableName}\'" + " table is dropped")
    def NoSuchTable():
        print(prompt+"No such table")
    def DropReferenceTableError(tableName):
        print(prompt+"Drop table has failed: " + f"\'{tableName}\'" + " is referenced by other table")
    def InsertResult():
        print(prompt+"The row is inserted")
    def SelectTableExistenceError(tableName):
        print(prompt+"Selection has failed: " + f"\'{tableName}\'" +" does not exist")
        

class MyTransformer(Transformer):       # Lark의 Transformer를 상속받아 각 query에 해당하는 내용을 출력
    def create_table_query(self, items):
        table_name = items[2].children[0].lower()   # items[2]는 table_name이므로, table_name을 소문자로 변환해 저장
        column_definition_iter = items[3].find_data("column_definition")    # items[3]은 column_definition이므로, column_definition을 찾아 저장
        primary_key_constraint_iter = items[3].find_data("primary_key_constraint")   # items[3]은 primary_key_constraint이므로, primary_key_constraint을 찾아 저장
        referential_constraint_iter = items[3].find_data("referential_constraint")  # items[3]은 referential_constraint이므로, referential_constraint을 찾아 저장
        table = myDB.get(bytes(table_name + "_schema", encoding='utf-8')) # table_name에 해당하는 schema를 가져와 table에 저장
        if table:   # table이 이미 존재하는 경우 TableExistenceError
            Messages.TableExistenceError()
            raise Exception
        schema = {"columnDef" : {}, "tableConstraint" : {}} # schema는 columnDef와 tableConstraint을 가지는 dictionary로 정의
        for item in column_definition_iter:  
            column_name = item.children[0].children[0].lower()  # column_name을 소문자로 변환해 저장
            if column_name in schema["columnDef"]:  # column_name이 이미 존재하는 경우 DuplicateColumnDefError
                Messages.DuplicateColumnDefError()
                raise Exception
            data_type = item.children[1].children[0].lower()  # data_type을 소문자로 변환해 저장
            if data_type=="char":   # data_type이 char인 경우
                char_len = int(item.children[1].children[2])
                if char_len < 1:    # char_len이 1보다 작은 경우 CharLengthError
                    Messages.CharLengthError()
                    raise Exception
                data_type = "char("+item.children[1].children[2]+")"    # data_type을 'char(char_len)'으로 변환하여 저장
            isNull = True
            if item.children[2] != None:    # not null인 경우, isNull을 False로 변환
                isNull = False
            schema["columnDef"][column_name] = {"data_type" : data_type, "isNull" : isNull} # schema의 columnDef에 해당 column의 data_type과 isNull을 저장
        for item in primary_key_constraint_iter:    # primary_key_constraint에 대한 처리
            if "primary_key" in schema["tableConstraint"]: # primary_key가 이미 존재하는 경우 DuplicatePrimaryKeyDefError
                Messages.DuplicatePrimaryKeyDefError()
                raise Exception
            column_name_nodes = item.children[2].find_data("column_name")
            pk_list = []
            for i in column_name_nodes: # primary_key에 해당하는 column_name을 찾아 pk_list에 저장
                column_name = i.children[0]
                if column_name in schema["columnDef"]:
                    pk_list.append(column_name)
                    schema["columnDef"][column_name]["isNull"] = False # primary_key에 해당하는 column은 not null로 변환
                else:
                    Messages.NonExistingColumnDefError(column_name) # column_name이 존재하지 않는 경우 NonExistingColumnDefError
                    raise Exception
            schema["tableConstraint"]["primary_key"] = pk_list # schema의 tableConstraint에 primary_key를 저장
        foreign_key_dict_list = []
        for item in referential_constraint_iter: # referential_constraint에 대한 처리
            fk_column_name_nodes = item.children[2].find_data("column_name") # foreign_key에 해당하는 column_name을 찾아 저장
            fk_list = []
            for i in fk_column_name_nodes:
                column_name = i.children[0]
                if column_name in schema["columnDef"]: # column_name이 schema에 존재하는 경우 fk_list에 저장
                    fk_list.append(column_name)
                else:
                    Messages.NonExistingColumnDefError(column_name) # column_name이 존재하지 않는 경우 NonExistingColumnDefError
                    raise Exception
            referential_table_name = item.children[4].children[0]
            referential_column_name_nodes = item.children[5].find_data("column_name")
            ref_key_list = []
            referential_schema = myDB.get(bytes(referential_table_name + "_schema", encoding='utf-8')) # 참조하는 table의 schema를 가져와 referential_schema에 저장
            if referential_schema == None: # 참조하는 table이 존재하지 않는 경우 ReferenceTableExistenceError
                Messages.ReferenceTableExistenceError()
                raise Exception
            referential_schema = json.loads(referential_schema.decode("utf-8")) # referential_schema를 dictionary로 변환
            primary_key_count = 0
            for index, i in enumerate(referential_column_name_nodes):
                ref_key_name = i.children[0]
                if ref_key_name not in referential_schema["columnDef"]: # 참조하는 column이 존재하지 않는 경우 ReferenceColumnExistenceError
                    Messages.ReferenceColumnExistenceError()
                    raise Exception
                if referential_schema["columnDef"][ref_key_name]["data_type"] != schema["columnDef"][fk_list[index]]["data_type"]: # 참조하는 column의 data_type이 다른 경우 ReferenceTypeError
                    Messages.ReferenceTypeError()
                    raise Exception
                if "primary_key" not in referential_schema["tableConstraint"] or ref_key_name not in referential_schema["tableConstraint"]["primary_key"]: # 참조하는 column이 primary_key가 아닌 경우 ReferenceNonPrimaryKeyError
                    Messages.ReferenceNonPrimaryKeyError()
                    raise Exception
                primary_key_count += 1
                ref_key_list.append(ref_key_name)
            if primary_key_count != len(referential_schema["tableConstraint"]["primary_key"]): # 참조하는 table의 primary_key의 개수와 참조하는 column의 개수가 다른 경우 ReferenceNonPrimaryKeyError
                Messages.ReferenceNonPrimaryKeyError()
                raise Exception
            foreign_key_dict_list.append({"foreign_key": fk_list, "reference_table": referential_table_name, "reference_key": ref_key_list})
        schema["tableConstraint"]["foreign_keys"] = foreign_key_dict_list
        schema_byte_data = json.dumps(schema).encode('utf-8')
        myDB.put(bytes(table_name+'_schema', encoding='utf-8'), schema_byte_data) # schema를 저장
        myDB.put(bytes(table_name, encoding='utf-8'), None) # table을 저장
        table_name_list = myDB.get(bytes("table_name_list", encoding='utf-8')) # table_name_list를 가져옴
        if not table_name_list: # table_name_list가 없는 경우 table_name_list를 생성
            table_name_list = [table_name]
        else: # table_name_list가 있는 경우 table_name_list에 table_name을 추가
            table_name_list = json.loads(table_name_list.decode('utf-8'))
            table_name_list.append(table_name)
        table_name_list = json.dumps(table_name_list).encode('utf-8')
        myDB.put(bytes("table_name_list", encoding='utf-8'), table_name_list) # table_name_list를 저장
        Messages.CreateTableSuccess(table_name) # table 생성 성공 메시지 출력

    def drop_table_query(self, items):
        table_name = items[2].children[0].lower()
        table_name_list = myDB.get(bytes("table_name_list", encoding='utf-8'))
        table_name_list = json.loads(table_name_list.decode('utf-8'))
        if not table_name_list or table_name not in table_name_list: # table_name이 존재하지 않는 경우 NoSuchTable 메시지 출력
            Messages.NoSuchTable()
            raise Exception
        for table_name_iter in table_name_list: # table_name이 참조하는 table이 있는 경우 DropReferenceTableError 메시지 출력
            if table_name==table_name_iter:
                continue
            other_table_schema = myDB.get(bytes(table_name_iter+'_schema', encoding='utf-8'))
            other_table_schema = json.loads(other_table_schema.decode('utf-8'))
            if "foreign_keys" in other_table_schema["tableConstraint"]:
                fk_list = other_table_schema["tableConstraint"]["foreign_keys"]
                for fk_info in fk_list:
                    if table_name == fk_info["reference_table"]:
                        Messages.DropReferenceTableError(table_name)
                        raise Exception
        table_name_list.remove(table_name) # table_name이 참조하는 table이 없는 경우 table_name_list에서 table_name을 제거
        table_name_list = json.dumps(table_name_list).encode('utf-8')
        myDB.put(bytes("table_name_list", encoding='utf-8'), table_name_list) # table_name_list를 저장
        myDB.put(bytes(table_name, encoding='utf-8'), None) # table을 None으로 저장
        myDB.put(bytes(table_name+'_schema', encoding='utf-8'), None) # schema를 None으로 저장
        Messages.DropSuccess(table_name) # table 삭제 성공 메시지 출력
        
    def explain_query(self, items):
        table_name = items[1].children[0].lower()
        table_name_list = myDB.get(bytes("table_name_list", encoding='utf-8'))
        table_name_list = json.loads(table_name_list.decode('utf-8'))
        if not table_name_list or table_name not in table_name_list: # table_name이 table_name_list에 존재하지 않는 경우 NoSuchTable 메시지 출력
            Messages.NoSuchTable()
            raise Exception
        schema = myDB.get(bytes(table_name+'_schema', encoding='utf-8'))
        schema = json.loads(schema.decode('utf-8'))
        print("-----------------------------------------------------------------")
        print(f"table_name [{table_name}]")
        print("{:<20} {:<10} {:<10} {:<10}".format("column_name", "type", "null", "key"))
        for column_name in schema["columnDef"]: # schema에 저장된 column에 대한 정보를 출력
            print("{:<20} ".format(column_name), end="") # column_name 출력
            print("{:<10} ".format(schema["columnDef"][column_name]["data_type"]), end="") # data_type 출력
            if schema["columnDef"][column_name]["isNull"]: # isNull이 True인 경우 Y, False인 경우 N 출력
                print("{:<10} ".format("Y"), end="")
            else:
                print("{:<10} ".format("N"), end="")
            is_fk = False
            is_pk = False
            if "foreign_keys" in schema["tableConstraint"]: # 해당 column이 foreign key에 포함된 경우 is_fk를 True로 변환
                for fk_info in schema["tableConstraint"]["foreign_keys"]:
                    if column_name in fk_info["foreign_key"]:
                        is_fk = True
                        break
            if "primary_key" in schema["tableConstraint"] and column_name in schema["tableConstraint"]["primary_key"]:
                is_pk = True # 해당 column이 primary key에 포함된 경우 is_pk를 True로 변환
            if is_pk and is_fk:
                print("{:<10} ".format("PRI/FOR"))
            elif is_pk and not is_fk:
                print("{:<10} ".format("PRI"))
            elif not is_pk and is_fk:
                print("{:<10} ".format("FOR"))
            else:
                print()
        print("-----------------------------------------------------------------")
            
    def describe_query(self, items): # explain_query와 동일
        table_name = items[1].children[0].lower()
        table_name_list = myDB.get(bytes("table_name_list", encoding='utf-8'))
        table_name_list = json.loads(table_name_list.decode('utf-8'))
        if not table_name_list or table_name not in table_name_list:
            Messages.NoSuchTable()
            raise Exception
        schema = myDB.get(bytes(table_name+'_schema', encoding='utf-8'))
        schema = json.loads(schema.decode('utf-8'))
        print("-----------------------------------------------------------------")
        print(f"table_name [{table_name}]")
        print("{:<20} {:<10} {:<10} {:<10}".format("column_name", "type", "null", "key"))
        for column_name in schema["columnDef"]:
            print("{:<20} ".format(column_name), end="")
            print("{:<10} ".format(schema["columnDef"][column_name]["data_type"]), end="")
            if schema["columnDef"][column_name]["isNull"]:
                print("{:<10} ".format("Y"), end="")
            else:
                print("{:<10} ".format("N"), end="")
            is_fk = False
            is_pk = False
            if "foreign_keys" in schema["tableConstraint"]:
                for fk_info in schema["tableConstraint"]["foreign_keys"]:
                    if column_name in fk_info["foreign_key"]:
                        is_fk = True
                        break
            if "primary_key" in schema["tableConstraint"] and column_name in schema["tableConstraint"]["primary_key"]:
                is_pk = True
            if is_pk and is_fk:
                print("{:<10} ".format("PRI/FOR"))
            elif is_pk and not is_fk:
                print("{:<10} ".format("PRI"))
            elif not is_pk and is_fk:
                print("{:<10} ".format("FOR"))
            else:
                print()
        print("-----------------------------------------------------------------")
    def desc_query(self, items): # explain_query와 동일
        table_name = items[1].children[0].lower()
        table_name_list = myDB.get(bytes("table_name_list", encoding='utf-8'))
        table_name_list = json.loads(table_name_list.decode('utf-8'))
        if not table_name_list or table_name not in table_name_list:
            Messages.NoSuchTable()
            raise Exception
        schema = myDB.get(bytes(table_name+'_schema', encoding='utf-8'))
        schema = json.loads(schema.decode('utf-8'))
        print("-----------------------------------------------------------------")
        print(f"table_name [{table_name}]")
        print("{:<20} {:<10} {:<10} {:<10}".format("column_name", "type", "null", "key"))
        for column_name in schema["columnDef"]:
            print("{:<20} ".format(column_name), end="")
            print("{:<10} ".format(schema["columnDef"][column_name]["data_type"]), end="")
            if schema["columnDef"][column_name]["isNull"]:
                print("{:<10} ".format("Y"), end="")
            else:
                print("{:<10} ".format("N"), end="")
            is_fk = False
            is_pk = False
            if "foreign_keys" in schema["tableConstraint"]:
                for fk_info in schema["tableConstraint"]["foreign_keys"]:
                    if column_name in fk_info["foreign_key"]:
                        is_fk = True
                        break
            if "primary_key" in schema["tableConstraint"] and column_name in schema["tableConstraint"]["primary_key"]:
                is_pk = True
            if is_pk and is_fk:
                print("{:<10} ".format("PRI/FOR"))
            elif is_pk and not is_fk:
                print("{:<10} ".format("PRI"))
            elif not is_pk and is_fk:
                print("{:<10} ".format("FOR"))
            else:
                print()
        print("-----------------------------------------------------------------")
    def insert_query(self, items):
        table_name = items[2].children[0].lower()
        table_name_list = myDB.get(bytes("table_name_list", encoding='utf-8'))
        table_name_list = json.loads(table_name_list.decode('utf-8'))
        if not table_name_list or table_name not in table_name_list: # table_name이 table_name_list에 존재하지 않는 경우 NoSuchTable 메시지 출력
            Messages.NoSuchTable()
            raise Exception
        schema = myDB.get(bytes(table_name+'_schema', encoding='utf-8'))
        schema = json.loads(schema.decode('utf-8'))
        table = myDB.get(bytes(table_name, encoding='utf-8'))
        if table: # table이 존재하는 경우 리스트로 변환
            table = json.loads(table.decode('utf-8'))
        else: # table이 존재하지 않는 경우 빈 리스트로 초기화
            table = []
        values = items[3].find_data("value") # insert할 value를 찾아 저장
        column_name_list = list(schema["columnDef"].keys()) # schema에 저장된 column_name을 리스트로 변환
        insert_column_name_list = items[3].find_data("column_name") # insert할 column_name을 찾아 저장
        insert_column_name_index = []
        for index, column_name in enumerate(insert_column_name_list):
            insert_column_name_index.append(column_name_list.index(column_name.children[0])) # insert할 column_name의 index를 schema의 column_name_list에서 찾아 저장
        if not insert_column_name_index:
            insert_column_name_index = list(range(0, len(column_name_list))) # insert할 column_name이 명시되지 않은 경우 schema의 column_name_list의 index를 그대로 저장
        value_cnt = 0
        for index, value in enumerate(values): # value의 data_type에 대해 확인
            if index == len(column_name_list):
                raise Exception
            value_type = value.children[0].type.lower()
            value_cnt += 1
            if value_type=="null" and schema["columnDef"][column_name_list[insert_column_name_index[index]]]["isNull"] == False:
                raise Exception
            if value_type=="int" and schema["columnDef"][column_name_list[insert_column_name_index[index]]]["data_type"] != "int":
                raise Exception
            if value_type=="str" and schema["columnDef"][column_name_list[insert_column_name_index[index]]]["data_type"][:4] != "char":
                raise Exception
            if value_type=="date" and schema["columnDef"][column_name_list[insert_column_name_index[index]]]["data_type"] != "date":
                raise Exception
        if value_cnt != len(column_name_list):
            raise Exception
        insert_value_list = []
        index = 0
        for value in items[3].find_data("value"):
            value_type = value.children[0].type.lower()
            if value_type == "str": # value의 data_type이 str인 경우 char_len을 확인하여 char_len보다 길면 char_len만큼만 저장
                char_len = int(schema["columnDef"][column_name_list[insert_column_name_index[index]]]["data_type"][5:-1])
                str_value = value.children[0][1:-1]
                insert_value_list.append(str_value[:char_len])
            elif value_type == "int": # value의 data_type이 int인 경우 int로 변환하여 저장
                insert_value_list.append(int(value.children[0]))
            else: # value의 data_type이 date인 경우 그대로 저장
                insert_value_list.append(value.children[0])
            index += 1
        value_list = insert_value_list[:]
        insert_value_list.sort(key=lambda i: insert_column_name_index[value_list.index(i)]) # insert할 column_name의 index에 따라 value를 정렬
        table.append(insert_value_list) # table에 insert_value_list를 추가
        table = json.dumps(table).encode('utf-8')
        myDB.put(bytes(table_name, encoding='utf-8'), table) # table을 저장
        Messages.InsertResult() # insert 성공 메시지 출력

    def delete_query(self, items):
        print(prompt + '\'DELETE\' requested')
    def select_query(self, items):
        from_table_list = items[2].children[0].find_data("table_name")
        table_name_list = myDB.get(bytes("table_name_list", encoding='utf-8'))
        table_name_list = json.loads(table_name_list.decode('utf-8'))
        from_table_name_list = []
        for table_name in from_table_list: # from_table_list에 있는 table_name이 table_name_list에 존재하는지 확인
            if not table_name_list or table_name.children[0].lower() not in table_name_list:
                Messages.SelectTableExistenceError(table_name.children[0].lower()) # table_name이 존재하지 않는 경우 SelectTableExistenceError
                raise Exception
            from_table_name_list.append(table_name.children[0].lower())
        table_name = from_table_name_list[0]
        schema = myDB.get(bytes(table_name+'_schema', encoding='utf-8'))
        schema = json.loads(schema.decode('utf-8'))
        table = myDB.get(bytes(table_name, encoding='utf-8'))
        if table: # table이 존재하는 경우 리스트로 변환
            table = json.loads(table.decode('utf-8'))
        else: # table이 존재하지 않는 경우 빈 리스트로 초기화
            table = []
        column_len_list = []
        for column_name in schema["columnDef"]: # column_name의 길이를 저장
            print("+"+"-"*(len(column_name)+2), end="")
            column_len_list.append(len(column_name))
        print("+")
        for column_name in schema["columnDef"]: # column_name을 출력
            print("| {} ".format(column_name.upper()), end="")
        print("|")
        for column_name in schema["columnDef"]: # column_name의 길이만큼 경계선 출력
            print("+"+"-"*(len(column_name)+2), end="")
        print("+")

        for row in table:
            for index, item in enumerate(row):
                print("| {:^{}} ".format(item, column_len_list[index]), end="")
            print("|")
        for column_name in schema["columnDef"]:
            print("+"+"-"*(len(column_name)+2), end="")
        print("+")
        
    def update_query(self, items):
        print(prompt + '\'UPDATE\' requested')
    def show_tables_query(self, items):
        print("-----------------------------------------------------------------")
        table_name_list = myDB.get(bytes("table_name_list", encoding='utf-8'))
        table_name_list = json.loads(table_name_list.decode('utf-8'))
        if table_name_list: # table_name_list에 있는 table_name을 출력
            for column_name in table_name_list:
                print(column_name)
        print("-----------------------------------------------------------------")
    def EXIT(self, items):
        exit()
    
prompt = "DB_2020-11782> "  # 학번이 들어간 프롬프트

myDB = db.DB()
if os.path.exists('myDB.db'):
    myDB.open('myDB.db', dbtype=db.DB_HASH)    
else:
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

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
        try:
            MyTransformer().transform(output) # query에 따라 알맞은 동작을 수행하는 MyTransformer 실행
        except Exception as e:
            break
        