import multiprocessing
import concurrent.futures
import elasticsearch
import json

# a simple elasticsearch ingester that takes source material (urls, files, strings), parses them into a python dictionary containing features of that material, and then pushes the dict to an ES instance.
# each source material is considered isolated, and the parsing can only result in ONE elasticsearch document.

# TODO: modularize, make the forwarder a module. this would allow forwarding into files etc.

class Ingester():
    def __init__(self, es_host, es_index, delete_index=False):
        self.es_host = es_host
        self.es_index = es_index
        self.es = elasticsearch.Elasticsearch(self.es_host)
        if self.es.indices.exists(self.es_index) and delete_index:
            self.es.indices.delete(self.es_index)
        if not self.es.indices.exists(self.es_index):
            self.es.indices.create(self.es_index)
        self.doctypes = dict()

        # multiprocess stuff
        self.running_ingests = []
        # worker threads are spawned when document ingestion is requested.

    def __enter__(self):
        return self

    def __exit__(self):
        # idk if this is needed
        for ingest in running_ingests:
            print("killing ingest...")
            ingest.terminate()

    def create_doctype(self, name, fields): # does not yet support nested fields
        request_body = {
            '_timestamp' : {
                'enabled' : 'false'
            },
            'properties' : fields
        }
        self.es.indices.put_mapping(index=self.es_index, doc_type=name, body=json.dumps(request_body))
        self.doctypes[name] = {'fields': fields} # this overrides old definitions! but double mappings would make the put_mapping fail anyways.

    def _submit_work(source_material, parser_function, worker_count, es_host, es_index, es_doctype):
        # elasticsearch client in main process
        es = elasticsearch.Elasticsearch(es_host)

        with concurrent.futures.ProcessPoolExecutor(worker_count) as pool:
            jobs=[]
            for mat in source_material:
                jobs.append(pool.submit(parser_function, mat))
            for future in concurrent.futures.as_completed(jobs):
                print("forwarding a document to ES")
                if future.done() and future.exception() == None:
                    es.index(index=es_index, doc_type=es_doctype, body=json.dumps(future.result()))

    # maybe provide an ingest function that uses elasticsearchs auto mapping...
    def ingest_documents(self, doctype, source_material, parser_function, worker_count=multiprocessing.cpu_count()):
        #if doctype not in self.doctypes:
        #    raise Exception("doctype ", doctype, "was not defined yet")
        # some more sanity checks? function callable, enough arguments?

        # we need to put the submitting into a new process, otherwise this will block until all work is done.
        p = multiprocessing.Process(target=Ingester._submit_work, args=(source_material, parser_function, worker_count, self.es_host, self.es_index, doctype,))
        self.running_ingests.append(p)
        p.start()

def example_feature_extractor(document):
    features = dict() # will have fields that correspond to my_doctype
    features['version'] = 3
    if "Hello" in document:
        features['another_field'] = True
    return features

if __name__ == "__main__":
    # always re-create testindex, good for debugging as you'll start with a fresh index
    i = Ingester('http://elastic_host:9200', 'testindex', delete_index=True)

    # define what fields your data type will have.
    # These are the features that you want to extract from the documents.
    # see https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping-intro.html
    # you can ingest data without defining a doctype - elasticsearch will guess
    my_doctype = dict()
    my_doctype['version'] = {'type':'string', 'index':'not_analyzed'}
    my_doctype['another_field'] = {'type':'boolean'}
    i.create_doctype("my_doctype", my_doctype)

    # this could be a list of filenames, urls, or even raw data.
    # Elements of this list will be passed as the only argument to your extractor
    document_list = ["Document_1","Document_2","Document_3"]

    # start ingesting
    i.ingest_documents("my_doctype", document_list, example_feature_extractor)
    print("it's not blocking :)")
