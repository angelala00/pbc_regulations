## pbc_regulations

#### crawler Typical workflow

python -m pbc_regulations.crawler --task zhengwugongkai_administrative_normative_documents --cache-start-page
python -m pbc_regulations.crawler --task zhengwugongkai_administrative_normative_documents --cache-listing --refresh-pages
python -m pbc_regulations.crawler --task zhengwugongkai_administrative_normative_documents --build-page-structure
python -m pbc_regulations.crawler --task zhengwugongkai_administrative_normative_documents --download-from-structure --verify-local

### Policy text extractor
python -m pbc_regulations.extractor.extract_policy_texts --stage-dedupe
python -m pbc_regulations.extractor.extract_policy_texts --task tiaofasi_normative_document --stage-extract --document-id tiaofasi_normative_document:408

### structure
python -m pbc_regulations.structure --format json


### Unified portal
python -m pbc_regulations --host 0.0.0.0 --port 8000



http://localhost:8000/api/policies
http://localhost:8000/api/policies?scope=all
http://localhost:8000/api/policies/中华人民共和国反洗钱法?include=meta
http://localhost:8000/api/policies/中华人民共和国反洗钱法?include=outline
http://localhost:8000/api/policies/中华人民共和国反洗钱法?include=text


http://localhost:8000/api/clause?title=中华人民共和国反洗钱法&item=第一条
http://localhost:8000/api/clause?key=《中华人民共和国反洗钱法》第一条
http://localhost:8000/api/clause?key=《中华人民共和国反洗钱法》第一条，第三条
http://localhost:8000/api/clause?key=《中华人民共和国反洗钱法》第一条，第三条，\n《中华人民共和国票据法》第（八）条
http://localhost:8000/api/clause?key=《中华人民共和国反洗钱法》第一条，第三条，\n《中华人民共和国票据法》第八条，第三款
http://localhost:8000/api/clause?key=《中华人民共和国反洗钱法》第四点，第五项

curl -N -X POST http://localhost:8000/api/legal_search/ai_chat   -H 'Content-Type: application/json'   -d '{ "query": "违规发行预付卡违反了什么法律？" }'

git remote add modelscope https://oauth2:ms-0d8f9c4e-4a3d-4058-86cf-62dd7ecdbda9@www.modelscope.cn/angelala00/icrawler.git
git push modelscope HEAD:github-master
