#!/usr/bin/env python3
"""Independent corpus counter and strict complete posting-list validator."""
import argparse, collections, hashlib, json, re
from pathlib import Path

def verify(documents, stopfile, output, sources=None):
    stops=set(stopfile.read_text(encoding='utf-8').split())
    expected=collections.defaultdict(dict)
    raw=filtered=0
    docs=sorted(documents.glob('*.txt'))
    assert len(docs)>=10, 'Need at least 10 documents'
    for doc in docs:
        words=re.findall('[a-z]+',doc.read_text(encoding='utf-8').lower())
        raw+=len(words)
        counts=collections.Counter(w for w in words if w not in stops)
        filtered+=sum(counts.values())
        for word,count in counts.items(): expected[word][doc.name]=count
    if sources:
        for source in json.loads(sources.read_text(encoding='utf-8')):
            assert hashlib.sha256((documents/source['document']).read_bytes()).hexdigest()==source['sha256']
    actual={}
    files=sorted(output.glob('part-r-*')) if output.is_dir() else [output]
    assert files, 'No reducer output'
    for part in files:
        last=''
        for line in part.read_text(encoding='utf-8').splitlines():
            word,value=line.split('\t',1)
            assert word not in actual and word>last, f'Duplicate/unsorted key {word}'
            last=word
            pairs=re.findall(r'<([^<>,;]+),(\d+)>',value)
            assert pairs and pairs[-1][0]=='total'
            assert value==';'.join(f'<{name},{count}>' for name,count in pairs)+'.'
            names=[name for name,_ in pairs[:-1]]
            assert names==sorted(set(names))
            postings={name:int(count) for name,count in pairs[:-1]}
            assert all(c>0 for c in postings.values())
            assert sum(postings.values())==int(pairs[-1][1])
            actual[word]=postings
    assert actual==dict(expected), 'Complete independent counter differs from Hadoop output'
    assert not stops.intersection(actual), 'Stopword leaked into index'
    summary=dict(documents=len(docs),stopwords=len(stops),tokens_seen=raw,stopwords_removed=raw-filtered,tokens_indexed=filtered,unique_words=len(actual),postings=sum(map(len,actual.values())),all_postings_match=True,all_totals_match=True,stopwords_absent=True)
    return summary

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--documents',type=Path,default=Path('data/documents'));p.add_argument('--stopwords',type=Path,default=Path('data/stopwords.txt'));p.add_argument('--output',type=Path,required=True);p.add_argument('--sources',type=Path,default=Path('data/sources.json'));p.add_argument('--summary',type=Path)
    a=p.parse_args();result=verify(a.documents,a.stopwords,a.output,a.sources)
    text=json.dumps(result,indent=2);print(text);print('ALL_INDEX_CHECKS_PASSED')
    if a.summary:a.summary.write_text(text+'\n',encoding='utf-8')
