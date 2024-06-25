from mysql.connector import connect
import pandas as pd
import numpy as np

def make_connection():
    success = False
    while not success: # db connection이 성공할 때까지 반복
        try:
            connection = connect(
                host='astronaut.snu.ac.kr',
                port=7001,
                user='DB2020_11782',
                password='DB2020_11782',
                db='DB2020_11782',
                charset='utf8'
            )
            success = True
        except:
            continue
    return connection

def initialize_database():
    connection = make_connection() # db connection 생성
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('drop table if exists borrow') # table이 이미 존재하면 삭제
        cursor.execute('drop table if exists rate')
        cursor.execute('drop table if exists user')
        cursor.execute('drop table if exists book')
        cursor.execute('create table user (id int primary key auto_increment , name varchar(10))') # table 생성
        cursor.execute('create table book (id int primary key auto_increment, title varchar(50), author varchar(30), available int default 1, unique(title, author))')
        cursor.execute('create table borrow (user_id int, book_id int, foreign key (user_id) references user(id), foreign key (book_id) references book(id))')
        cursor.execute('create table rate (user_id int, book_id int, rating int, foreign key (user_id) references user(id), foreign key (book_id) references book(id))')
        data = pd.read_csv('data.csv', encoding='cp949') # data.csv 파일을 읽어옴
        book_id_list = []
        user_id_list = []
        for i in range(len(data)): # data를 하나씩 읽어서 db에 insert
            b_id = int(data['b_id'][i])
            b_title = data['b_title'][i]
            b_author = data['b_author'][i]
            u_id = int(data['u_id'][i])
            u_name = data['u_name'][i]
            b_u_rating = int(data['b_u_rating'][i])
            if(b_id not in book_id_list): # book_id가 중복되지 않도록 체크
                book_id_list.append(b_id)
                cursor.execute('insert into book (id, title, author, available) values (%s, %s, %s, 1)', (b_id, b_title, b_author))
            if(u_id not in user_id_list): # user_id가 중복되지 않도록 체크
                user_id_list.append(u_id)
                cursor.execute('insert into user (id, name) values (%s, %s)', (u_id, u_name))
            cursor.execute('insert into rate (user_id, book_id, rating) values (%s, %s, %s)', (u_id, b_id, b_u_rating))
        connection.commit() # db에 반영
    connection.close()
    print('Database successfully initialized') # 초기화 완료 메시지 출력
    

def reset():
    ask = input('Are you sure you want to reset the database? (y/n): ') # 사용자에게 db 초기화에 대한 확인 메시지 출력
    if ask == 'y':
        connection = make_connection()
        with connection.cursor() as cursor:
            cursor.execute('delete from borrow') # table의 모든 데이터 삭제
            cursor.execute('delete from rate')
            cursor.execute('delete from user')
            cursor.execute('delete from book')
            cursor.execute('drop table if exists borrow') # table이 이미 존재하면 삭제
            cursor.execute('drop table if exists rate') # 이후로 initialize_database()와 동일
            cursor.execute('drop table if exists user')
            cursor.execute('drop table if exists book')
            cursor.execute('create table user (id int primary key auto_increment , name varchar(10))') # table 생성
            cursor.execute('create table book (id int primary key auto_increment, title varchar(50), author varchar(30), available int default 1, unique(title, author))')
            cursor.execute('create table borrow (user_id int, book_id int, foreign key (user_id) references user(id), foreign key (book_id) references book(id))')
            cursor.execute('create table rate (user_id int, book_id int, rating int, foreign key (user_id) references user(id), foreign key (book_id) references book(id))')
            data = pd.read_csv('data.csv', encoding='cp949') # data.csv 파일을 읽어옴
            book_id_list = []
            user_id_list = []
            for i in range(len(data)): # data를 하나씩 읽어서 db에 insert
                b_id = int(data['b_id'][i])
                b_title = data['b_title'][i]
                b_author = data['b_author'][i]
                u_id = int(data['u_id'][i])
                u_name = data['u_name'][i]
                b_u_rating = int(data['b_u_rating'][i])
                if(b_id not in book_id_list): # book_id가 중복되지 않도록 체크
                    book_id_list.append(b_id)
                    cursor.execute('insert into book (id, title, author, available) values (%s, %s, %s, 1)', (b_id, b_title, b_author))
                if(u_id not in user_id_list): # user_id가 중복되지 않도록 체크
                    user_id_list.append(u_id)
                    cursor.execute('insert into user (id, name) values (%s, %s)', (u_id, u_name))
                cursor.execute('insert into rate (user_id, book_id, rating) values (%s, %s, %s)', (u_id, b_id, b_u_rating))
            connection.commit() # db에 반영
        connection.close()
        print('Database successfully initialized') # 초기화 완료 메시지 출력
    

def print_books():
    connection = make_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute('''select id, title, author, avg(rating), available 
                    from book left join rate on book.id = rate.book_id 
                    group by book.id order by book.id
                    ''')
    result = cursor.fetchall() # book table에서 모든 데이터를 가져옴
    print('-'*110) # 출력 형식에 맞게 출력
    print('{:<5} {:<50} {:<30} {:<10} {:<10}'.format('id', 'title', 'author', 'avg.rating', 'quantity'))
    print('-'*110)
    for row in result:
        if row['avg(rating)'] == None: # rating이 없는 경우 None으로 출력
            print('{:<5} {:<50} {:<30} {:<10} {:<10}'.format(str(row['id']), row['title'], row['author'], 'None', str(row['available'])))
        else:
            print('{:<5} {:<50} {:<30} {:<10} {:<10}'.format(str(row['id']), row['title'], row['author'], str(row['avg(rating)']), str(row['available'])))
    print('-'*110)
    connection.close()

def print_users():
    connection = make_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute('select * from user order by id') # user table에서 모든 데이터를 가져옴
    result = cursor.fetchall()
    print('-'*110) # 출력 형식에 맞게 출력
    print('{:<5} {:<10}'.format('id', 'name'))
    print('-'*110)
    for row in result:
        print('{:<5} {:<10}'.format(row['id'], row['name']))
    print('-'*110)
    connection.close()

def insert_book():
    title = input('Book title: ')
    author = input('Book author: ')
    if len(title)<1 or len(title)>50: # title의 길이가 1~50 사이인지 체크
        print('Title length should range from 1 to 50 characters')
        return
    if len(author)<1 or len(author)>30: # author의 길이가 1~30 사이인지 체크
        print('Author length should range from 1 to 30 characters')
        return
    connection = make_connection()
    cursor = connection.cursor()
    cursor.execute('select * from book where title = %s and author = %s', (title, author)) # title과 author가 이미 존재하는지 체크
    cursor.fetchall()
    if cursor.rowcount > 0: # 이미 존재하는 경우 에러 메시지 출력
        print('Book (%s, %s) already exists' % (title, author))
        connection.close()
        return
    cursor.execute('insert into book (title, author) values (%s, %s)', (title, author)) # 존재하지 않는 경우 insert
    cursor.fetchall()
    connection.commit()
    connection.close()
    print('One book successfully inserted')
    

def remove_book():
    book_id = input('Book ID: ')
    connection = make_connection()
    cursor = connection.cursor()
    cursor.execute('select * from book where id = %s', (book_id, )) # book_id가 존재하는지 체크
    book = cursor.fetchall()
    if len(book) == 0: # 존재하지 않는 경우 에러 메시지 출력
        print('Book %s does not exist' % book_id)
        connection.close()
        return
    if book[0][3] == 0: # 현재 대출 중인 경우 에러 메시지 출력
        print('Cannot delete a book that is currently borrowed')
        connection.close()
        return
    cursor.execute('delete from rate where book_id = %s', (book_id, )) # book_id에 대한 모든 rate 삭제
    cursor.execute('delete from borrow where book_id = %s', (book_id, )) # book_id에 대한 모든 borrow 삭제
    cursor.execute('delete from book where id = %s', (book_id, )) # book_id에 대한 book 삭제
    connection.commit()
    connection.close()
    print('One book successfully removed')


def insert_user():
    name = input('User name: ')
    connection = make_connection()
    cursor = connection.cursor()
    if len(name)<1 or len(name)>10:
        print("Username length should range from 1 to 10 characters") # name의 길이가 1~10 사이인지 체크
        connection.close()
        return
    cursor.execute('insert into user (name) values (%s)', (name, )) # user insert
    connection.commit()
    connection.close()
    print('One user successfully inserted')

def remove_user():
    user_id = input('User ID: ')
    connection = make_connection()
    cursor = connection.cursor()
    cursor.execute('select * from user where id = %s', (user_id, )) # user_id가 존재하는지 체크
    user = cursor.fetchall()
    if len(user) == 0: # 존재하지 않는 경우 에러 메시지 출력
        print('User %s does not exist' % user_id)
        connection.close()
        return
    cursor.execute('select * from borrow where user_id = %s', (user_id, )) # user_id에 대한 borrow가 있는지 체크
    borrow = cursor.fetchall()
    if len(borrow) > 0: # borrow가 있는 경우 에러 메시지 출력
        print('Cannot delete a user with borrowed books')
        connection.close()
        return
    cursor.execute('delete from rate where user_id = %s', (user_id, )) # user_id에 대한 모든 rate 삭제
    cursor.execute('delete from user where id = %s', (user_id, )) # user_id에 대한 user 삭제
    connection.commit()
    connection.close()
    print('One user successfully removed')
    

def checkout_book():
    book_id = input('Book ID: ')
    user_id = input('User ID: ')
    connection = make_connection()
    cursor = connection.cursor()
    cursor.execute('select * from book where id = %s', (book_id, )) # book_id가 존재하는지 체크
    book = cursor.fetchall()
    if len(book) == 0: # 존재하지 않는 경우 에러 메시지 출력
        print('Book %s does not exist' % book_id)
        connection.close()
        return
    cursor.execute('select * from user where id = %s', (user_id, )) # user_id가 존재하는지 체크
    user = cursor.fetchall()
    if len(user) == 0: # 존재하지 않는 경우 에러 메시지 출력
        print('User %s does not exist' % user_id)
        connection.close()
        return
    if book[0][3] == 0: # 대출 중인 경우 에러 메시지 출력
        print('Cannot check out a book that is currently borrowed')
        connection.close()
        return
    cursor.execute('select * from borrow where user_id = %s', (user_id, )) # user_id에 대한 borrow가 있는지 체크
    borrow = cursor.fetchall()
    if len(borrow) == 2: # borrow가 2개인 경우 에러 메시지 출력
        print('User %s exceeded the maximum borrowing limit' % user_id)
        connection.close()
        return
    cursor.execute('update book set available = 0 where id = %s', (book_id, )) # book_id에 대한 대출 가능 권수를 0으로 변경
    cursor.execute('insert into borrow (user_id, book_id) values (%s, %s)', (user_id, book_id)) # 대출 정보 insert
    connection.commit()
    connection.close()
    print('Book successfully checked out')

def return_and_rate_book():
    book_id = input('Book ID: ')
    user_id = input('User ID: ')
    rating = input('Ratings (1~5): ')
    connection = make_connection()
    cursor = connection.cursor()
    cursor.execute('select * from book where id = %s', (book_id, )) # book_id가 존재하는지 체크
    book = cursor.fetchall()
    if len(book) == 0: # 존재하지 않는 경우 에러 메시지 출력
        print('Book %s does not exist' % book_id)
        connection.close()
        return
    cursor.execute('select * from user where id = %s', (user_id, )) # user_id가 존재하는지 체크
    user = cursor.fetchall()
    if len(user) == 0: # 존재하지 않는 경우 에러 메시지 출력
        print('User %s does not exist' % user_id)
        connection.close()
        return
    if int(rating) < 1 or int(rating) > 5: # rating이 1~5 사이인지 체크
        print('Rating should range from 1 to 5.')
        connection.close()
        return
    cursor.execute('select * from borrow where user_id = %s and book_id = %s', (user_id, book_id)) # user_id가 book_id를 대출했는지 체크
    borrow = cursor.fetchall()
    if len(borrow) == 0: # 대출하지 않은 경우 에러 메시지 출력
        print('Cannot return and rate a book that is not currently borrowed for this user')
        connection.close()
        return
    cursor.execute('update book set available = 1 where id = %s', (book_id, )) # book_id에 대한 대출 가능 권수를 1로 변경
    cursor.execute('delete from borrow where user_id = %s and book_id = %s', (user_id, book_id)) # borrow 삭제
    cursor.execute('delete from rate where user_id = %s and book_id = %s', (user_id, book_id)) # rate 삭제
    cursor.execute('insert into rate (user_id, book_id, rating) values (%s, %s, %s)', (user_id, book_id, rating)) # 평점 추가
    connection.commit()
    connection.close()
    print('Book successfully returned and rated')
    

def print_borrowing_status_for_user():
    user_id = input('User ID: ')
    connection = make_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute('select * from user where id = %s', (user_id, )) # user_id가 존재하는지 체크
    user = cursor.fetchall()
    if len(user) == 0: # 존재하지 않는 경우 에러 메시지 출력
        print('User %s does not exist' % user_id)
        connection.close()
        return
    cursor.execute('''select book.id, title, author, avg(rating) as avg_rating
                   from book join borrow on book.id = borrow.book_id 
                   left join rate on book.id = rate.book_id
                   where borrow.user_id = %s 
                   group by book.id
                   order by book.id''', (user_id, )) # user_id가 대출한 모든 책에 대한 정보를 가져옴
    result = cursor.fetchall()
    print('-'*110) # 출력 형식에 맞게 출력
    print('{:<5} {:<50} {:<30} {:<10}'.format('id', 'title', 'author', 'avg.rating'))
    print('-'*110)
    for row in result:
        if row['avg_rating'] == None: # rating이 없는 경우 None으로 출력
            print('{:<5} {:<50} {:<30} {:<10}'.format(str(row['id']), row['title'], row['author'], 'None'))
        else:
            print('{:<5} {:<50} {:<30} {:<10}'.format(str(row['id']), row['title'], row['author'], str(row['avg_rating'])))
    print('-'*110)
    connection.close()

def search_books():
    query = input('Query: ')
    connection = make_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute('''select id, title, author, avg(rating), available 
                   from book left join rate on book.id = rate.book_id 
                   where title like %s
                   group by book.id 
                   order by book.id''', ('%' + query + '%',)) # title에 query가 포함된 모든 책에 대한 정보를 가져옴
    result = cursor.fetchall()
    print('-'*110) # 출력 형식에 맞게 출력
    print('{:<5} {:<50} {:<30} {:<10} {:<10}'.format('id', 'title', 'author', 'avg.rating', 'quantity'))
    print('-'*110)
    for row in result:
        if row['avg(rating)'] == None: # rating이 없는 경우 None으로 출력
            print('{:<5} {:<50} {:<30} {:<10} {:<10}'.format(str(row['id']), row['title'], row['author'], 'None', str(row['available'])))
        else:
            print('{:<5} {:<50} {:<30} {:<10} {:<10}'.format(str(row['id']), row['title'], row['author'], str(row['avg(rating)']), str(row['available'])))
    print('-'*110)


def recommend_popularity():
    user_id = input('User ID: ')
    connection = make_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute('select * from user where id = %s', (user_id, )) # user_id가 존재하는지 체크
    user = cursor.fetchall()
    if len(user) == 0: # 존재하지 않는 경우 에러 메시지 출력
        print('User %s does not exist' % user_id)
        connection.close()
        return
    cursor.execute('''select id, title, author, avg(rating) as avg_rating
                   from book left join rate on book.id = rate.book_id 
                   where book.id not in (select book_id from rate where user_id = %s) 
                   and (select avg(rating) from rate where book_id = book.id) = (select max(avg_rating) 
                    from (select book_id, avg(rating) as avg_rating 
                    from rate where book_id not in (select book_id from rate where user_id = %s)
                    group by book_id) as temp)
                    group by book.id
                    order by book.id''', (user_id, user_id)) # user가 평가하지 않은 책 중 평점이 가장 높은 책을 가져옴
    row = cursor.fetchall()
    print('-'*110) # 출력 형식에 맞게 출력
    print("Rating-based")
    print('-'*110)
    print("{:<5} {:<50} {:<30} {:<10}".format('id', 'title', 'author', 'avg.rating'))
    print('-'*110)
    if len(row) != 0:
        row = row[0]
        print('{:<5} {:<50} {:<30} {:<10}'.format(str(row['id']), row['title'], row['author'], str(row['avg_rating'])))
    cursor.execute('''select id, title, author, avg(rating) as avg_rating
                     from book left join rate on book.id = rate.book_id
                        where book.id not in (select book_id from rate where user_id = %s)
                        and (select count(*) from rate where book_id = book.id) = (select max(count)
                        from (select book_id, count(*) as count 
                            from rate where book_id not in (select book_id from rate where user_id = %s) 
                            group by book_id) as temp)
                        group by book.id
                        order by book.id''', (user_id, user_id)) # user가 평가하지 않은 책 중 평점을 많이 받은 책을 가져옴
    row = cursor.fetchall()
    print('-'*110) # 출력 형식에 맞게 출력
    print("Popularity-based")
    print('-'*110)
    print("{:<5} {:<50} {:<30} {:<10}".format('id', 'title', 'author', 'avg.rating'))
    print('-'*110)
    if len(row) != 0:
        row = row[0]
        print('{:<5} {:<50} {:<30} {:<10}'.format(str(row['id']), row['title'], row['author'], str(row['avg_rating'])))
    print('-'*110)
    connection.close()

    

def recommend_item_based():
    user_id = input('User ID: ')
    connection = make_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute('select * from user where id = %s', (user_id, )) # user_id가 존재하는지 체크
    user = cursor.fetchall()
    if len(user) == 0: # 존재하지 않는 경우 에러 메시지 출력
        print('User %s does not exist' % user_id)
        connection.close()
        return
    cursor.execute('''select user.id as user_id, book.id as book_id, rating 
                   from user 
                   cross join book 
                   left join rate on book.id = rate.book_id and user.id = rate.user_id
                   order by user.id, book.id''') # user와 book를 cross join하여 user가 평가한 rating을 가져옴
    data = cursor.fetchall()
    cursor.execute('select id from user order by id')
    user_id_list = cursor.fetchall() # user_id를 가져옴
    user_id_list = [row['id'] for row in user_id_list]
    cursor.execute('select id from book order by id')
    book_id_list = cursor.fetchall() # book_id를 가져옴
    book_id_list = [row['id'] for row in book_id_list]
    user_rate_list = []
    user_item_matrix = [] # user-item matrix를 생성
    for user in range(len(user_id_list)): # 평점이 없는 부분에 대해 임시 평점을 계산 후 반영
        rate_sum = 0
        cnt = 0
        for book in range(len(book_id_list)): # user의 평균 평점 계산
            if data[user * len(book_id_list) + book]['rating'] != None:
                rate_sum += data[user * len(book_id_list) + book]['rating']
                cnt += 1
        if cnt != 0:
            rate_avg = rate_sum / cnt
        else:
            rate_avg = 0
        row = []
        for book in range(len(book_id_list)): # user-item matrix의 row 생성
            if user_id_list.index(int(user_id)) == user: # user_id의 평점 원본을 따로 저장
                user_rate_list.append(data[user * len(book_id_list) + book]['rating'])
            if data[user * len(book_id_list) + book]['rating'] == None: # 평점이 없는 경우 임시 평점을 반영
                data[user * len(book_id_list) + book]['rating'] = rate_avg
            row.append(data[user * len(book_id_list) + book]['rating'])
        row = np.array(row)
        user_item_matrix.append(row)
    user_item_matrix = np.array(user_item_matrix) # user-item matrix 생성 완료
    similarity_matrix = [] # similarity matrix 생성
    for A in range(len(user_id_list)): # user 간의 cosine similarity 계산
        row = []
        for B in range(len(user_id_list)):
            similarity = 0
            if np.linalg.norm(user_item_matrix[A]) == 0 or np.linalg.norm(user_item_matrix[B]) == 0:
                similarity = 0
            else:
                similarity = np.dot(user_item_matrix[A], user_item_matrix[B])/(np.linalg.norm(user_item_matrix[A])*np.linalg.norm(user_item_matrix[B]))
            row.append(similarity)
        row = np.array(row)
        similarity_matrix.append(row)
    similarity_matrix = np.array(similarity_matrix) # similarity matrix 생성 완료
    candidate_books = [] # user가 평가하지 않은 책 중 추천할 책 후보 생성
    for i in range(len(book_id_list)):
        if user_rate_list[i] == None: # user가 평가하지 않은 책에 대해 예상 평점 계산
            numerator = 0
            denominator = 0
            for j in range(len(user_id_list)):
                if user_id_list[j] == int(user_id):
                    continue
                numerator += similarity_matrix[user_id_list.index(int(user_id))][j] * user_item_matrix[j][i]
                denominator += similarity_matrix[user_id_list.index(int(user_id))][j]
            if denominator == 0:
                candidate_books.append((book_id_list[i], 0))
            else:
                candidate_books.append((book_id_list[i], numerator/denominator)) # 예상 평점을 반영하여 후보에 추가
    if len(candidate_books) == 0:
        print('-'*110) # 출력 형식에 맞게 출력
        print('{:<5} {:<50} {:<30} {:<10} {:<10}'.format('id', 'title', 'author', 'avg.rating', 'exp.rating'))
        print('-'*110)
        print('-'*110)
        return
    max_rating_book = candidate_books[0]
    for i in range(1, len(candidate_books)): # 예상 평점이 가장 높은 책을 추천
        if candidate_books[i][1] > max_rating_book[1]:
            max_rating_book = candidate_books[i]
        elif candidate_books[i][1] == max_rating_book[1]: # 예상 평점이 같은 경우 book_id가 작은 책을 추천
            if candidate_books[i][0] < max_rating_book[0]:
                max_rating_book = candidate_books[i]
    cursor.execute('''select id, title, author, avg(rating) as avg_rating 
                   from book left join rate on book.id = rate.book_id 
                   where book.id = %s 
                   group by book.id''', (max_rating_book[0], )) # 추천된 책에 대한 정보를 가져옴
    result = cursor.fetchall()[0]
    print('-'*110) # 출력 형식에 맞게 출력
    print('{:<5} {:<50} {:<30} {:<10} {:<10}'.format('id', 'title', 'author', 'avg.rating', 'exp.rating'))
    print('-'*110)
    print('{:<5} {:<50} {:<30} {:<10} {:<10}'.format(str(result['id']), result['title'], result['author'], str(result['avg_rating']), str(max_rating_book[1])))
    print('-'*110)
    


def main():
    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all books')
        print('3. print all users')
        print('4. insert a new book')
        print('5. remove a book')
        print('6. insert a new user')
        print('7. remove a user')
        print('8. check out a book')
        print('9. return and rate a book')
        print('10. print borrowing status of a user')
        print('11. search books')
        print('12. recommend a book for a user using popularity-based method')
        print('13. recommend a book for a user using user-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_books()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_book()
        elif menu == 5:
            remove_book()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            checkout_book()
        elif menu == 9:
            return_and_rate_book()
        elif menu == 10:
            print_borrowing_status_for_user()
        elif menu == 11:
            search_books()
        elif menu == 12:
            recommend_popularity()
        elif menu == 13:
            recommend_item_based()
        elif menu == 14:
            print('Bye!')
            break
        elif menu == 15:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()
