## pbc_regulations

#### crawler Typical workflow

python -m pbc_regulations.crawler --task zhengwugongkai_administrative_normative_documents --cache-start-page
python -m pbc_regulations.crawler --task zhengwugongkai_administrative_normative_documents --cache-listing --refresh-pages
python -m pbc_regulations.crawler --task tiaofasi_normative_document --build-page-structure
python -m pbc_regulations.crawler --task tiaofasi_normative_document --download-from-structure --verify-local

### Policy text extractor
python -m pbc_regulations.extractor.extract_policy_texts --stage-dedupe
python -m pbc_regulations.extractor.extract_policy_texts --task tiaofasi_departmental_rule --stage-extract
python -m pbc_regulations.extractor.extract_policy_texts --stage-extract --document-id tiaofasi_departmental_rule:50 --force-reextract

### structure
python -m pbc_regulations.structure --stage-fill-info --doc-id zhengwugongkai_administrative_normative_documents:23
python -m pbc_regulations.structure --stage-output --format summary-only

### check
python scripts/check_extract_uniq.py --min-chars 150 --min-meaningful 60 --attachment-ratio 0.5

### Unified portal
python -m pbc_regulations --host 0.0.0.0 --port 8000



http://localhost:8000/api/policies/catalog?view=ai
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
http://localhost:8000/api/clause?keys=《中华人民共和国反洗钱法》第一条&keys=《中华人民共和国票据法》第八条，第三款

中国人民银行公告〔2015〕第43号（非银行支付机构网络支付业务管理办法）：第十五条

curl -N -X POST http://localhost:8000/api/legal_search/ai_chat   -H 'Content-Type: application/json'   -d '{ "query": "hello", "stream": "True" }'
curl -N -X POST http://localhost:8000/api/legal_search/ai_chat   -H 'Content-Type: application/json'   -d '{ "query": "违规发行预付卡违反了什么法律？", "stream": "True" }'

git remote add modelscope https://oauth2:ms-0d8f9c4e-4a3d-4058-86cf-62dd7ecdbda9@www.modelscope.cn/angelala00/icrawler.git
git push modelscope HEAD:github-master

test