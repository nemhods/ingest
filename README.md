# Overview

``ingest`` is a simple helper script to put documents into an [elasticsearch](https://www.elastic.co/) cluster.

Quick How To:
* have Python 3 with the module ``elasticsearch``
* have access to an elasticsearch cluster (via simple API, no fancy auth supported currently)
* a set of documents (filenames, URLs, raw python strings)
* a function that extracts features from these documents

``ingest`` then wraps the extractor function in multiprocessing, and automatically passes the extracted features into elasticsearch.

Possible improvements:
* make the forwarding to a database modular, so that forwarding to other dbs or even files is possible.
* the multiprocessing stuff does not feel optimised yet.

## Usage

```python
from ingest import Ingester

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
```
