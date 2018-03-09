from sqlalchemy import update
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
import pandas as pd

username = ''
password = ''
host = '127.0.0.1:3306'
database=''


def import_engine():
    return create_engine('mysql+mysqlconnector://'+username+':'+password+'@'+host+'/'+database+'')

def sql_read(query):
    engine=import_engine()
    connection = engine.connect()
    all_data = pd.read_sql(query,con=connection)
    connection.close()
    return all_data

def sql_update(data):
    engine = import_engine()
    connection = engine.connect()
    metadata = MetaData(engine)
    commentmeta = Table('wp_commentmeta', metadata, autoload=True)
    for index, row in data.iterrows():
        stmt = update(commentmeta).where(commentmeta.c.meta_id == row['meta_id']).values(meta_value=row['meta_value'])
        connection.execute(stmt)
    connection.close()


all_data=sql_read('SELECT * FROM wp_commentmeta')


evaluations = pd.DataFrame(all_data.meta_value.str.split(',').tolist(),columns =['kokybe','kaina','sparta','aptarnavimas'])
evaluations['kokybe']=evaluations['kokybe'].replace('1|0','1|1')
evaluations['kaina']=evaluations['kaina'].replace('2|0','2|1')
evaluations['sparta']=evaluations['sparta'].replace('3|0','3|1')
evaluations['aptarnavimas']=evaluations['aptarnavimas'].replace('4|0','4|1')
evaluations['meta_value'] = evaluations[['kokybe', 'kaina','sparta','aptarnavimas']].apply(lambda x: ','.join(x), axis=1)
all_data['meta_value']=evaluations['meta_value']

sql_update(all_data)

