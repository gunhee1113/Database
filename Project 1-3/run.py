from lark import Lark, Transformer
from berkeleydb import db
import sys, os
import json
from datetime import date

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
    def SelectTableExistenceError(tableName):
        print(prompt+"Selection has failed: " + f"\'{tableName}\'" +" does not exist")
    def InsertResult():
        print(prompt+"1 row inserted")
    def InsertTypeMismatchError():
        print(prompt+"Insertion has failed: Types are not matched")
    def InsertColumnExistenceError(colName):
        print(prompt+"Insertion has failed: " + f"\'{colName}\'" + " does not exist")
    def InsertColumnNonNullableError(colName):
        print(prompt+"Insertion has failed: " + f"\'{colName}\'" + " is not nullable")
    def DeleteResult(count):
        print(prompt+f"{count} row(s) deleted")
    def SelectColumnResolveError(colName):
        print(prompt+f"Selection has failed: fail to resolve \'{colName}\'")
    def WhereIncomparableError():
        print(prompt+"Where clause trying to compare incomparable values")
    def WhereTableNotSpecified():
        print(prompt+"Where clause trying to reference tables which are not specified")
    def WhereColumnNotExist():
        print(prompt+"Where clause trying to reference non existing column")
    def WhereAmbiguousReference():
        print(prompt+"Where clause contains ambiguous reference")

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
                column_name = i.children[0].lower()
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
                column_name = i.children[0].lower()
                if column_name in schema["columnDef"]: # column_name이 schema에 존재하는 경우 fk_list에 저장
                    fk_list.append(column_name)
                else:
                    Messages.NonExistingColumnDefError(column_name) # column_name이 존재하지 않는 경우 NonExistingColumnDefError
                    raise Exception
            referential_table_name = item.children[4].children[0].lower()
            referential_column_name_nodes = item.children[5].find_data("column_name")
            ref_key_list = []
            referential_schema = myDB.get(bytes(referential_table_name + "_schema", encoding='utf-8')) # 참조하는 table의 schema를 가져와 referential_schema에 저장
            if referential_schema == None: # 참조하는 table이 존재하지 않는 경우 ReferenceTableExistenceError
                Messages.ReferenceTableExistenceError()
                raise Exception
            referential_schema = json.loads(referential_schema.decode("utf-8")) # referential_schema를 dictionary로 변환
            primary_key_count = 0
            for index, i in enumerate(referential_column_name_nodes):
                ref_key_name = i.children[0].lower()
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
            if column_name.children[0] not in column_name_list: # insert할 column_name이 schema에 존재하지 않는 경우 InsertColumnExistenceError
                Messages.InsertColumnExistenceError(column_name.children[0])
                raise Exception
            insert_column_name_index.append(column_name_list.index(column_name.children[0])) # insert할 column_name의 index를 schema의 column_name_list에서 찾아 저장
        if not insert_column_name_index:
            insert_column_name_index = list(range(0, len(column_name_list))) # insert할 column_name이 명시되지 않은 경우 schema의 column_name_list의 index를 그대로 저장
        if len(insert_column_name_index) > len(column_name_list): # insert할 column_name의 개수가 schema의 column_name의 개수와 다른 경우 InsertTypeMismatchError
            Messages.InsertTypeMismatchError()
            raise Exception
        value_cnt = 0
        for index, value in enumerate(values): # value의 data_type에 대해 확인
            if index == len(column_name_list):
                Messages.InsertTypeMismatchError() # insert할 table의 column_name의 개수보다 많은 value가 있는 경우 InsertTypeMismatchError
                raise Exception
            value_type = value.children[0].type.lower()
            value_cnt += 1
            if value_type=="null" and schema["columnDef"][column_name_list[insert_column_name_index[index]]]["isNull"] == False:
                Messages.InsertColumnNonNullableError(column_name_list[insert_column_name_index[index]]) # insert할 data가 null인데 column이 not null인 경우 InsertColumnNonNullableError
                raise Exception
            if value_type=="int" and schema["columnDef"][column_name_list[insert_column_name_index[index]]]["data_type"] != "int":
                Messages.InsertTypeMismatchError() # insert할 data는 int인데 column의 data_type이 int가 아닌 경우 InsertTypeMismatchError
                raise Exception
            if value_type=="str" and schema["columnDef"][column_name_list[insert_column_name_index[index]]]["data_type"][:4] != "char":
                Messages.InsertTypeMismatchError() # insert할 data는 str인데 column의 data_type이 char가 아닌 경우 InsertTypeMismatchError
                raise Exception
            if value_type=="date" and schema["columnDef"][column_name_list[insert_column_name_index[index]]]["data_type"] != "date":
                Messages.InsertTypeMismatchError() # insert할 data는 date인데 column의 data_type이 date가 아닌 경우 InsertTypeMismatchError
                raise Exception
        if value_cnt != len(insert_column_name_index):
            Messages.InsertTypeMismatchError() # insert할 table의 column_name의 개수보다 적은 value가 있는 경우 InsertTypeMismatchError
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
            elif value_type == "date":
                insert_value_list.append(value.children[0]) # value의 data_type이 date인 경우 date로 변환하여 저장
            else: # value의 data_type이 null인 경우 None으로 저장
                insert_value_list.append(None)
            index += 1
        value_list = []
        for i in range(len(column_name_list)):
            if i not in insert_column_name_index:
                if schema["columnDef"][column_name_list[i]]["isNull"]:
                    value_list.append(None)
                else:
                    Messages.InsertTypeMismatchError() # insert할 column_name이 명시되지 않았는데 not null인 경우 InsertTypeMismatchError
                    raise Exception
            else:
                value_list.append(insert_value_list[insert_column_name_index.index(i)]) # insert할 value를 column_name_list의 순서에 맞게 value_list에 저장 
        table.append(value_list) # table에 value_list를 추가
        table = json.dumps(table).encode('utf-8')
        myDB.put(bytes(table_name, encoding='utf-8'), table) # table을 저장
        Messages.InsertResult() # insert 성공 메시지 출력

    def delete_query(self, items):
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
            Messages.DeleteResult(0)
            return
        where_clause = items[3]
        if not where_clause:
            deleted_rows = len(table)
            table = []
            table = json.dumps(table).encode('utf-8')
            myDB.put(bytes(table_name, encoding='utf-8'), table) # table을 저장
            Messages.DeleteResult(deleted_rows)
            return
        bool_expr = items[3].children[1]
        bool_term = bool_expr.find_data("boolean_term")
        condition_list = []
        num_term = 0
        for term in bool_term:
            num_term += 1
            bool_factor = term.find_data("boolean_factor")
            for factor in bool_factor:
                condition_info = {}
                predicate_iter = factor.find_data("predicate")
                for predicate in predicate_iter:
                    for _ in predicate.find_data("comparison_predicate"):
                        comp_operand_1_type = ""
                        comp_operand_2_type = ""
                        comp_operand_1 = predicate.children[0].children[0]
                        if len(comp_operand_1.children)==2:
                            comp_operand_1_table_name = comp_operand_1.children[0]
                            if comp_operand_1_table_name and comp_operand_1_table_name.children[0].lower() != table_name:
                                Messages.WhereTableNotSpecified()
                                raise Exception
                            comp_operand_1_column_name = comp_operand_1.children[1].children[0].lower()
                            if comp_operand_1_column_name not in schema["columnDef"]:
                                Messages.WhereColumnNotExist()
                                raise Exception
                            comp_operand_1 = comp_operand_1_column_name
                            comp_operand_1_type = "column_name"
                        else:
                            comp_operand_1_type = comp_operand_1.children[0].children[0].type.lower()
                            if comp_operand_1_type == "int":
                                comp_operand_1 = int(comp_operand_1.children[0].children[0])
                            elif comp_operand_1_type == "str":
                                comp_operand_1 = comp_operand_1.children[0].children[0][1:-1]
                            elif comp_operand_1_type == "date":
                                comp_operand_1 = date.fromisoformat(comp_operand_1.children[0].children[0])
                            else:
                                comp_operand_1 = comp_operand_1.children[0].children[0]
                        comp_op = predicate.children[0].children[1].children[0].value
                        comp_operand_2 = predicate.children[0].children[2]
                        if len(comp_operand_2.children)==2:
                            comp_operand_2_table_name = comp_operand_2.children[0]
                            if comp_operand_2_table_name and comp_operand_2_table_name.children[0].lower() != table_name:
                                Messages.WhereTableNotSpecified()
                                raise Exception
                            comp_operand_2_column_name = comp_operand_2.children[1].children[0].lower()
                            if comp_operand_2_column_name not in schema["columnDef"]:
                                Messages.WhereColumnNotExist()
                                raise Exception
                            comp_operand_2 = comp_operand_2_column_name
                            comp_operand_2_type = "column_name"
                        else:
                            comp_operand_2_type = comp_operand_2.children[0].children[0].type.lower()
                            if comp_operand_2_type == "int":
                                comp_operand_2 = int(comp_operand_2.children[0].children[0])
                            elif comp_operand_2_type == "str":
                                comp_operand_2 = comp_operand_2.children[0].children[0][1:-1]
                            elif comp_operand_2_type == "date":
                                comp_operand_2 = date.fromisoformat(comp_operand_2.children[0].children[0])
                            else:
                                comp_operand_2 = comp_operand_2.children[0].children[0]
                        condition_info["predicate"] = "comparison"
                        condition_info["comp_operand_1"] = comp_operand_1
                        condition_info["comp_operand_1_type"] = comp_operand_1_type
                        condition_info["comp_op"] = comp_op
                        condition_info["comp_operand_2"] = comp_operand_2
                        condition_info["comp_operand_2_type"] = comp_operand_2_type
                        if comp_operand_1_type == "column_name":
                            comp_operand_1_type = schema["columnDef"][condition_info["comp_operand_1"]]["data_type"]
                        if comp_operand_2_type == "column_name":
                            comp_operand_2_type = schema["columnDef"][condition_info["comp_operand_2"]]["data_type"]
                        if comp_operand_1_type != comp_operand_2_type:
                            if (len(comp_operand_1_type) >= 4 and comp_operand_1_type[:4] == "char" or comp_operand_1_type == "str") and (len(comp_operand_2_type) >= 4 and comp_operand_2_type[:4] == "char" or comp_operand_2_type == "str"):
                                pass
                            else:
                                Messages.WhereIncomparableError()
                                raise Exception
                        if (len(comp_operand_1_type) >= 4 and comp_operand_1_type[:4] == "char" or comp_operand_1_type == "str") and condition_info["comp_op"] not in ['=', '!=']:
                            Messages.WhereIncomparableError()
                            raise Exception
                        elif comp_operand_1_type == 'int' and condition_info["comp_op"] not in ['=', '!=', '<', '>', '<=', '>=']:
                            Messages.WhereIncomparableError()
                            raise Exception
                        elif comp_operand_1_type == 'date' and condition_info["comp_op"] not in ['=', '!=', '<', '>', '<=', '>=']:
                            Messages.WhereIncomparableError()
                            raise Exception

                    for _ in predicate.find_data("null_predicate"):
                        null_pred_table_name = predicate.children[0].children[0]
                        if null_pred_table_name and null_pred_table_name != table_name:
                            Messages.WhereTableNotSpecified()
                            raise Exception
                        column_name = predicate.children[0].children[1].children[0].lower()
                        if column_name not in schema["columnDef"]:
                            Messages.WhereColumnNotExist()
                            raise Exception
                        is_null = predicate.children[0].children[2].children[1] is None
                        condition_info["predicate"] = "null"
                        condition_info["column_name"] = column_name
                        condition_info["is_null"] = is_null
                pred_positive = factor.children[0] is None
                condition_info["pred_positive"] = pred_positive
                condition_list.append(condition_info)
        deleted_rows_num = 0
        column_name_list = list(schema["columnDef"].keys())
        delete_row_list = []
        for condition in condition_list:
            to_delete = []
            if condition["predicate"] == "comparison":
                comp_operand_1 = condition["comp_operand_1"]
                comp_op = condition["comp_op"]
                comp_operand_2 = condition["comp_operand_2"]
                for row in table:
                    if condition["comp_operand_1_type"] == "column_name":
                        value_1 = row[column_name_list.index(comp_operand_1)]
                        if schema["columnDef"][comp_operand_1]["data_type"] == "date":
                            value_1 = date.fromisoformat(value_1)
                    else:
                        value_1 = comp_operand_1
                    if condition["comp_operand_2_type"] == "column_name":
                        value_2 = row[column_name_list.index(comp_operand_2)]
                        if schema["columnDef"][comp_operand_2]["data_type"] == "date":
                            value_2 = date.fromisoformat(value_2)
                    else:
                        value_2 = comp_operand_2
                    if comp_op == "=" and value_1 == value_2:
                        to_delete.append(row)
                    elif comp_op == "!=" and value_1 != value_2:
                        to_delete.append(row)
                    elif comp_op == "<" and value_1 < value_2:
                        to_delete.append(row)
                    elif comp_op == ">" and value_1 > value_2:
                        to_delete.append(row)
                    elif comp_op == "<=" and value_1 <= value_2:
                        to_delete.append(row)
                    elif comp_op == ">=" and value_1 >= value_2:
                        to_delete.append(row)
                        
            elif condition["predicate"] == "null":
                column_name = condition["column_name"]
                is_null = condition["is_null"]
                for row in table:
                    if is_null and row[column_name_list.index(column_name)] is None:
                        to_delete.append(row)
                    elif not is_null and row[column_name_list.index(column_name)] is not None:
                        to_delete.append(row)
            if condition["pred_positive"]:
                delete_row_list.append(to_delete)
            else:
                delete_row_list.append([x for x in table if x not in to_delete])
        if num_term == 1:
            if len(delete_row_list) == 1:
                deleted_rows_num = len(delete_row_list[0])
                table = [x for x in table if x not in delete_row_list[0]]
            else:
                deleting_rows = [x for x in table if x in delete_row_list[0] and x in delete_row_list[1]]
                deleted_rows_num = len(deleting_rows)
                table = [x for x in table if x not in deleting_rows]
        if num_term == 2:
            deleting_rows = [x for x in table if x in delete_row_list[0] or x in delete_row_list[1]]
            deleted_rows_num = len(deleting_rows)
            table = [x for x in table if x not in deleting_rows]
        table = json.dumps(table).encode('utf-8')
        myDB.put(bytes(table_name, encoding='utf-8'), table) # table을 저장
        Messages.DeleteResult(deleted_rows_num) # delete 성공 메시지 출력

    def select_query(self, items):
        from_table_list = [x for x in items[2].children[0].find_data("table_name")]
        table_name_list = myDB.get(bytes("table_name_list", encoding='utf-8'))
        table_name_list = json.loads(table_name_list.decode('utf-8'))
        from_table_name_list = []
        for table_name in from_table_list: # from_table_list에 있는 table_name이 table_name_list에 존재하는지 확인
            if not table_name_list or table_name.children[0].lower() not in table_name_list:
                Messages.SelectTableExistenceError(table_name.children[0].lower()) # table_name이 존재하지 않는 경우 SelectTableExistenceError
                raise Exception
            from_table_name_list.append(table_name.children[0].lower())
        schema_list = []
        table_list = []
        for table_name in from_table_name_list:
            schema = myDB.get(bytes(table_name+'_schema', encoding='utf-8'))
            schema = json.loads(schema.decode('utf-8'))
            schema_list.append(schema)
            table = myDB.get(bytes(table_name, encoding='utf-8'))
            if table: # table이 존재하는 경우 리스트로 변환
                table = json.loads(table.decode('utf-8'))
            else: # table이 존재하지 않는 경우 빈 리스트로 초기화
                table = []
            table_list.append(table)
        select_table_column_name_list = []
        select_table = []
        for index, schema in enumerate(schema_list):
            column_name_list = list(schema["columnDef"].keys())
            for column_name in column_name_list:
                select_table_column_name_list.append(from_table_name_list[index] + "." + column_name)
            if select_table:
                new_select_table = []
                for row in table_list[index]:
                    new_select_table += [x + row for x in select_table]
                select_table = new_select_table
            else:
                select_table = [x for x in table_list[index]]
        where_clause = [x for x in items[2].find_data("where_clause")]
        select_list = [x for x in items[1].find_data("selected_column")]
        select_column_name_list = []
        if not select_list: # select_list에 *가 있는 경우 모든 column을 출력
            select_column_name_list = select_table_column_name_list
        else: 
            for selected_column in select_list:
                selected_column_table_name = selected_column.children[0]
                selected_column_name = selected_column.children[1].children[0].lower()
                if selected_column_table_name:
                    if selected_column_table_name.children[0].lower() not in from_table_name_list:
                        Messages.SelectColumnResolveError(selected_column_table_name.children[0].lower())
                        raise Exception
                    else:
                        selected_column_table_name = selected_column_table_name.children[0].lower()
                        for column_name in select_table_column_name_list:
                            if selected_column_table_name == column_name.split(".")[0] and selected_column_name == column_name.split(".")[1]:
                                select_column_name_list.append(column_name)
                else:
                    column_exist = False
                    for column_name in select_table_column_name_list:
                        if selected_column_name == column_name.split(".")[1]:
                            if column_exist:
                                Messages.SelectColumnResolveError(selected_column_name)
                                raise Exception
                            else:
                                column_exist = True
                                select_column_name_list.append(column_name)
                    if not column_exist:
                        Messages.SelectColumnResolveError(selected_column_name)
                        raise Exception      
        if not where_clause:
            column_len_list = []
            for column_name in select_column_name_list: 
                print("+"+"-"*(len(column_name)+2), end="")
                column_len_list.append(len(column_name))
            print("+")
            for column_name in select_column_name_list: # column_name을 출력
                print("| {} ".format(column_name.upper()), end="")
            print("|")
            for column_name in select_column_name_list: # column_name의 길이만큼 경계선 출력
                print("+"+"-"*(len(column_name)+2), end="")
            print("+")
            for row in select_table:
                for index, item in enumerate(row):
                    if select_table_column_name_list[index] in select_column_name_list:
                        if item is None:
                            item = "null"
                        print("| {:^{}} ".format(item, column_len_list[select_column_name_list.index(select_table_column_name_list[index])]), end="")
                print("|")
            for column_name in select_column_name_list:
                print("+"+"-"*(len(column_name)+2), end="")
            print("+")
        else:
            bool_expr = where_clause[0].children[1]
            bool_term = bool_expr.find_data("boolean_term")
            condition_list = []
            num_term = 0
            for term in bool_term:
                num_term += 1
                bool_factor = term.find_data("boolean_factor")
                for factor in bool_factor:
                    condition_info = {}
                    predicate_iter = factor.find_data("predicate")
                    for predicate in predicate_iter:
                        for _ in predicate.find_data("comparison_predicate"):
                            comp_operand_1_type = ""
                            comp_operand_2_type = ""
                            comp_operand_1 = predicate.children[0].children[0]
                            if len(comp_operand_1.children)==2:
                                comp_operand_1_table_name = comp_operand_1.children[0]
                                if comp_operand_1_table_name and comp_operand_1_table_name.children[0].lower() not in from_table_name_list:
                                    Messages.WhereTableNotSpecified()
                                    raise Exception
                                comp_operand_1_column_name = comp_operand_1.children[1].children[0].lower()
                                if comp_operand_1_table_name:
                                    comp_operand_1_table_name = comp_operand_1_table_name.children[0].lower()
                                    if comp_operand_1_table_name + "." + comp_operand_1_column_name not in select_table_column_name_list:
                                        Messages.WhereColumnNotExist()
                                        raise Exception
                                    comp_operand_1 = comp_operand_1_table_name + "." + comp_operand_1_column_name
                                else:
                                    column_exist = False
                                    comp_operand_1_table_name = ""
                                    for column_name in select_table_column_name_list:
                                        if comp_operand_1_column_name == column_name.split(".")[1]:
                                            if column_exist:
                                                Messages.WhereAmbiguousReference()
                                                raise Exception
                                            else:
                                                column_exist = True
                                                comp_operand_1_table_name = column_name.split(".")[0]
                                    if not column_exist:
                                        Messages.WhereColumnNotExist()
                                        raise Exception
                                    comp_operand_1 = comp_operand_1_table_name + "." + comp_operand_1_column_name
                                comp_operand_1_type = "column_name"
                            else:
                                comp_operand_1_type = comp_operand_1.children[0].children[0].type.lower()
                                if comp_operand_1_type == "int":
                                    comp_operand_1 = int(comp_operand_1.children[0].children[0])
                                elif comp_operand_1_type == "str":
                                    comp_operand_1 = comp_operand_1.children[0].children[0][1:-1]
                                elif comp_operand_1_type == "date":
                                    comp_operand_1 = date.fromisoformat(comp_operand_1.children[0].children[0])
                                else:
                                    comp_operand_1 = comp_operand_1.children[0].children[0]
                            comp_op = predicate.children[0].children[1].children[0].value
                            comp_operand_2 = predicate.children[0].children[2]
                            if len(comp_operand_2.children)==2:
                                comp_operand_2_table_name = comp_operand_2.children[0]
                                if comp_operand_2_table_name and comp_operand_2_table_name.children[0].lower() not in from_table_name_list:
                                    Messages.WhereTableNotSpecified()
                                    raise Exception
                                comp_operand_2_column_name = comp_operand_2.children[1].children[0].lower()
                                if comp_operand_2_table_name:
                                    comp_operand_2_table_name = comp_operand_2_table_name.children[0].lower()
                                    if comp_operand_2_table_name + "." + comp_operand_2_column_name not in select_table_column_name_list:
                                        Messages.WhereColumnNotExist()
                                        raise Exception
                                    comp_operand_2 = comp_operand_2_table_name + "." + comp_operand_2_column_name
                                else:
                                    column_exist = False
                                    comp_operand_2_table_name = ""
                                    for column_name in select_table_column_name_list:
                                        if comp_operand_2_column_name == column_name.split(".")[1]:
                                            if column_exist:
                                                Messages.WhereAmbiguousReference()
                                                raise Exception
                                            else:
                                                column_exist = True
                                                comp_operand_2_table_name = column_name.split(".")[0]
                                    if not column_exist:
                                        Messages.WhereColumnNotExist()
                                        raise Exception
                                    comp_operand_2 = comp_operand_2_table_name + "." + comp_operand_2_column_name
                                comp_operand_2_type = "column_name"
                            else:
                                comp_operand_2_type = comp_operand_2.children[0].children[0].type.lower()
                                if comp_operand_2_type == "int":
                                    comp_operand_2 = int(comp_operand_2.children[0].children[0])
                                elif comp_operand_2_type == "str":
                                    comp_operand_2 = comp_operand_2.children[0].children[0][1:-1]
                                elif comp_operand_2_type == "date":
                                    comp_operand_2 = date.fromisoformat(comp_operand_2.children[0].children[0])
                                else:
                                    comp_operand_2 = comp_operand_2.children[0].children[0]
                            condition_info["predicate"] = "comparison"
                            condition_info["comp_operand_1"] = comp_operand_1
                            condition_info["comp_operand_1_type"] = comp_operand_1_type
                            condition_info["comp_op"] = comp_op
                            condition_info["comp_operand_2"] = comp_operand_2
                            condition_info["comp_operand_2_type"] = comp_operand_2_type
                            if comp_operand_1_type == "column_name":
                                comp_operand_1_type = schema_list[from_table_name_list.index(comp_operand_1.split(".")[0])]["columnDef"][comp_operand_1.split(".")[1]]["data_type"]
                            if comp_operand_2_type == "column_name":
                                comp_operand_2_type = schema_list[from_table_name_list.index(comp_operand_2.split(".")[0])]["columnDef"][comp_operand_2.split(".")[1]]["data_type"]
                            if comp_operand_1_type != comp_operand_2_type:
                                if (len(comp_operand_1_type) >= 4 and comp_operand_1_type[:4] == "char" or comp_operand_1_type == "str") and (len(comp_operand_2_type) >= 4 and comp_operand_2_type[:4] == "char" or comp_operand_2_type == "str"):
                                    pass
                                else:
                                    Messages.WhereIncomparableError()
                                    raise Exception
                            if (len(comp_operand_1_type) >= 4 and comp_operand_1_type[:4] == "char" or comp_operand_1_type == "str") and condition_info["comp_op"] not in ['=', '!=']:
                                Messages.WhereIncomparableError()
                                raise Exception
                            elif comp_operand_1_type == 'int' and condition_info["comp_op"] not in ['=', '!=', '<', '>', '<=', '>=']:
                                Messages.WhereIncomparableError()
                                raise Exception
                            elif comp_operand_1_type == 'date' and condition_info["comp_op"] not in ['=', '!=', '<', '>', '<=', '>=']:
                                Messages.WhereIncomparableError()
                                raise Exception
                            
                        for _ in predicate.find_data("null_predicate"):
                            null_pred_table_name = predicate.children[0].children[0]
                            if null_pred_table_name and null_pred_table_name != table_name:
                                Messages.WhereTableNotSpecified()
                                raise Exception
                            null_pred_column_name = predicate.children[0].children[1].children[0].lower()
                            if null_pred_table_name:
                                if null_pred_table_name + "." + null_pred_column_name not in select_table_column_name_list:
                                    Messages.WhereColumnNotExist()
                                    raise Exception
                                null_pred_column_name = null_pred_table_name + "." +null_pred_column_name
                            else:
                                column_exist = False
                                null_pred_table_name = ""
                                for column_name in select_table_column_name_list:
                                    if column_name.split(".")[1] == null_pred_column_name:
                                        if column_exist:
                                            Messages.WhereAmbiguousReference()
                                            raise Exception
                                        else:
                                            column_exist = True
                                            null_pred_table_name = column_name.split(".")[0]
                                if not column_exist:
                                    Messages.WhereColumnNotExist()
                                    raise Exception
                                null_pred_column_name = null_pred_table_name + "." + null_pred_column_name

                            is_null = predicate.children[0].children[2].children[1] is None
                            condition_info["predicate"] = "null"
                            condition_info["column_name"] = null_pred_column_name
                            condition_info["is_null"] = is_null
                    pred_positive = factor.children[0] is None
                    condition_info["pred_positive"] = pred_positive
                    condition_list.append(condition_info)
            selected_table_list = []
            for condition in condition_list:
                to_select = []
                if condition["predicate"] == "comparison":
                    comp_operand_1 = condition["comp_operand_1"]
                    comp_op = condition["comp_op"]
                    comp_operand_2 = condition["comp_operand_2"]
                    for row in select_table:
                        if condition["comp_operand_1_type"] == "column_name":
                            value_1 = row[select_table_column_name_list.index(comp_operand_1)]
                            if schema_list[from_table_name_list.index(comp_operand_1.split(".")[0])]["columnDef"][comp_operand_1.split(".")[1]]["data_type"] == "date":
                                value_1 = date.fromisoformat(value_1)
                        else:
                            value_1 = comp_operand_1
                        if condition["comp_operand_2_type"] == "column_name":
                            value_2 = row[select_table_column_name_list.index(comp_operand_2)]
                            if schema_list[from_table_name_list.index(comp_operand_2.split(".")[0])]["columnDef"][comp_operand_2.split(".")[1]]["data_type"] == "date":
                                value_2 = date.fromisoformat(value_2)
                        else:
                            value_2 = comp_operand_2
                        if comp_op == "=" and value_1 == value_2:
                            to_select.append(row)
                        elif comp_op == "!=" and value_1 != value_2:
                            to_select.append(row)
                        elif comp_op == "<" and value_1 < value_2:
                            to_select.append(row)
                        elif comp_op == ">" and value_1 > value_2:
                            to_select.append(row)
                        elif comp_op == "<=" and value_1 <= value_2:
                            to_select.append(row)
                        elif comp_op == ">=" and value_1 >= value_2:
                            to_select.append(row)
                elif condition["predicate"] == "null":
                    column_name = condition["column_name"]
                    is_null = condition["is_null"]
                    for row in select_table:
                        if is_null and row[select_table_column_name_list.index(column_name)] is None:
                            to_select.append(row)
                        elif not is_null and row[select_table_column_name_list.index(column_name)] is not None:
                            to_select.append(row)
                if condition["pred_positive"]:
                    selected_table_list.append(to_select)
                else:
                    selected_table_list.append([x for x in select_table if x not in to_select])
            if num_term == 1:
                if len(selected_table_list) == 1:
                    select_table = selected_table_list[0]
                else:
                    select_table = [x for x in select_table if x in selected_table_list[0] and x in selected_table_list[1]]
            if num_term == 2:
                select_table = [x for x in select_table if x in selected_table_list[0] or x in selected_table_list[1]]
            column_len_list = []
            for column_name in select_column_name_list: 
                print("+"+"-"*(len(column_name)+2), end="")
                column_len_list.append(len(column_name))
            print("+")
            for column_name in select_column_name_list: # column_name을 출력
                print("| {} ".format(column_name.upper()), end="")
            print("|")
            for column_name in select_column_name_list: # column_name의 길이만큼 경계선 출력
                print("+"+"-"*(len(column_name)+2), end="")
            print("+")
            for row in select_table:
                for index, item in enumerate(row):
                    if select_table_column_name_list[index] in select_column_name_list:
                        if item is None:
                            item = "null"
                        print("| {:^{}} ".format(item, column_len_list[select_column_name_list.index(select_table_column_name_list[index])]), end="")
                print("|")
            for column_name in select_column_name_list:
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
        