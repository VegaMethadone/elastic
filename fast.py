from elasticsearch import Elasticsearch, helpers
import json
from tqdm import tqdm
from pymystem3 import Mystem # https://github.com/nlpub/pymystem3

INDEX_NAME = "wikipedia_paragraphs"
INDEX_NAME_MORF = "wikipedia_morphologic"

def Load_Data(path):
    with open(path, "r", encoding="utf-8") as file:
        paragraphs = json.load(file)
        return paragraphs

def Index_data_by_elastic(es, data, index):
    actions = [
        {
            "_index": index,
            "_id": para["uid"],  
            "_source": {
                "ru_wiki_pageid": para["ru_wiki_pageid"],
                "text": para["text"]
            }
        }
        for para in data
    ]

    helpers.bulk(es, actions)
    print(f"Индекс {index} успешно создан с {len(actions)} документами!")

def create_index(es):
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(
            index=INDEX_NAME,
            body={
                "mappings": {
                    "properties": {
                        "ru_wiki_pageid": {"type": "integer"},
                        "text": {"type": "text"}
                    }
                }
            }
        )
        print(f"Индекс {INDEX_NAME} создан.")
    else:
        print(f"Индекс {INDEX_NAME} уже существует.")


def create_index_with_morphological_preprocessing(es):
    if not es.indices.exists(index=INDEX_NAME_MORF):
        resp = es.indices.create(
            index=INDEX_NAME_MORF,
            body={
                "settings": {
                    "analysis": {
                        "filter": {
                            "russian_stop": {
                                "type": "stop",
                                "stopwords": "_russian_"
                            },
                            "russian_keywords": {
                                "type": "keyword_marker",
                                "keywords": [
                                    "пример"
                                ]
                            },
                            "russian_stemmer": {
                                "type": "stemmer",
                                "language": "russian"
                            }
                        },
                        "analyzer": {
                            "rebuilt_russian": {
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "russian_stop",
                                    "russian_keywords",
                                    "russian_stemmer"
                                ]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "ru_wiki_pageid": {"type": "integer"},
                        "text": {"type": "text", "analyzer": "rebuilt_russian"}
                    }
                }
            }
        )

        print(f"Индекс {INDEX_NAME_MORF} создан.")
    else:
        print(f"Индекс {INDEX_NAME_MORF} уже существует.")




def contains(target, arr):
    return target in arr


def prepare_data(query, is_morfologic):

    if is_morfologic:
        m = Mystem()
        lemmas = m.lemmatize(query)
        query = "".join(lemmas)
    
    return {
        "query": {
            "match": {
                "text": query
            }
        },
        "size": 5
    }

    

def test_queries(es, test_data, amount_docs, index, is_morfologic = False):

    # Precision P  = tp / (tp + fp)
    # Recall R = tp / (tp + fn)

    precision = []
    recall = []
    mrr = []
    
    tp = 0 # релевантные документы
    fp = 0 # количество ненужных документов, которые были возвращены в результате поиска
    fn = 0 # количество релевантных документов, которые система не смогла обнаружить
    tn = 0 # количество документов, которые не являются релевантными, и система правильно не возвращает их

    for query_data in tqdm(test_data, desc=f"Processing queries for index: {index}"):
        query = query_data["question_text"]
        with_answer = query_data["paragraphs_uids"]["with_answer"]
        if  len(with_answer) == 0:
            continue
        
        results = es.search(index=index, body=prepare_data(query, is_morfologic))

        size = 5 # размер запроса
        ctp = 0 # кол. релевантных документов для данного запроса
        # print(results["hits"])

        # для MRR фиксирую первую позицию найденного документам
        first_occurance = True
        pos = 1
        for hit in results["hits"]["hits"]:
            id = hit["_id"]
            if contains(int(id), with_answer):
                ctp += 1
                if first_occurance:
                    mrr.append(1 / pos)
                    first_occurance = False
            pos += 1
        
        if first_occurance == True:
            mrr.append(0)

        cfn = len(with_answer) - ctp # сколько правильных ответов - сколько нашёл
        ctn = amount_docs - size + ctp # всего документов - размер запроса + релевантные
        
        # Precision P  = tp / (tp + fp)
        local_precision = ctp / size
        precision.append(local_precision)

        # Recall R = tp / (tp + fn)
        local_recall = ctp / (ctp + cfn)
        recall.append(local_recall)

    
    print("MAP:\t", sum(precision) / len(precision))
    print("MAR:\t", sum(recall) / len(recall))
    print("MRR:\t", sum(mrr) / len(mrr))


def main():
    print("Программа запускается...")

    es = Elasticsearch("http://localhost:9200")
    
    create_index(es)
    create_index_with_morphological_preprocessing(es)
    

    data_file_path = "RuBQ_2.0_paragraphs.json"
    data = Load_Data(data_file_path)
    
    Index_data_by_elastic(es, data, INDEX_NAME)
    Index_data_by_elastic(es, data,  INDEX_NAME_MORF)

    resp1 = es.count(index=INDEX_NAME)
    resp2 = es.count(index=INDEX_NAME_MORF)

    print(f"Количество записей в индексе {INDEX_NAME}: {resp1['count']}")
    print(f"Количество записей в индексе {INDEX_NAME_MORF}: {resp2['count']}\n")
    
    test_data = Load_Data("RuBQ_2.0_test.json")
    test_queries(es, test_data, resp1['count'], INDEX_NAME, False)
    test_queries(es, test_data, resp2['count'], INDEX_NAME_MORF, True)

    print("Программа завершила выполнение.")


if __name__ == "__main__":
    main()
