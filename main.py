from Graph_Storge import *
from Graph_Get_Json import *
from Graph_Retrieval import *

driver = None
try:
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
    driver.verify_connectivity() # 尝试连接并验证
    print("Neo4j 数据库连接成功！")
except Exception as e:
    print(f"无法连接到 Neo4j 数据库: {e}")
    print("请确保 Neo4j 正在运行，并且连接配置 (URI, 用户名, 密码) 正确。")
    exit() # 如果连接失败，后续导入操作将无法进行，直接退出

GOOGLE_API_KEY = '' #Put your api

if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)
    try:
        # 尝试列出模型，确认API Key有效
        models = genai.list_models()
        print("Gemini client initialized from environment variable.")
        gemini_model = genai.GenerativeModel('gemini-2.5-flash') # 或 'gemini-1.5-pro-latest' 如果需要更强的模型
    except Exception as e:
        print(f"Warning: Gemini API Key found, but client init/model list failed: {e}")
        print("Please check your GOOGLE_API_KEY and network connection.")
        gemini_model = None
else:
    print("Warning: GOOGLE_API_KEY environment variable not set. Cannot use Gemini API.")
    gemini_model = None

# --- 加载 NER 模型 ---
try:
    nlp = spacy.load("zh_core_web_sm")
    print("spaCy zh_core_web_sm loaded successfully.")
except Exception as e:
    print(f"Error loading spaCy model: {e}")
    print("Please run 'python -m spacy download zh_core_web_sm'")
    nlp = None

sample_txt_path = 'data/sample_document.txt'


def Build_base(content_path, nlp, gemini_model):
    docx_to_txt(content_path, sample_txt_path)
    document_content = load_text_file(sample_txt_path)
    processed_sentences = preprocess_text_optimized_v2(document_content, nlp)
    process_and_save(processed_sentences, nlp, gemini_model)

def Storge(extracted_path, driver):
    import_knowledge_to_neo4j(extracted_path, driver)

def Search(Sent_to_ask, driver, gemini_model):
    detailed_schema, schema_error = get_detailed_neo4j_schema(driver)
    ask_question(Sent_to_ask, detailed_schema, schema_error, gemini_model, driver)


# Build_base(r"E:\Code\Proj1\data\2024-2027年度重庆联通综合代维服务采购.docx",nlp, gemini_model)
# Storge(r"E:\Code\Proj1\extracted_knowledge.json", driver)
Search(["我们要维护什么"], driver, gemini_model)