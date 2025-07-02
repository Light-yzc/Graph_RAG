from Graph_Storge import *
from Graph_Get_Json import *
from Graph_Retrieval import *
USERNAME = 'neo4j'
PASSWORD = 'qwerqwer233'
driver = None
URI = 'bolt://localhost:7687'
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
    nlp = spacy.load("zh_core_web_trf")
    print("spaCy zh_core_web_trf loaded successfully.")
except Exception as e:
    print(f"Error loading spaCy model: {e}")
    print("Please run 'python -m spacy download zh_core_web_trf'")
    nlp = None

sample_txt_path = 'data/sample_document.txt'


def Build_base(content_path, sample_txt_path, nlp, gemini_model):
    # docx_to_txt(content_path, sample_txt_path)
    docx_to_markdown(content_path, sample_txt_path)
    document_content = load_text_file(sample_txt_path)
    processed_sentences = preprocess_text_optimized_v2(document_content, nlp)
    process_and_save(processed_sentences, nlp, gemini_model)

def Storge(extracted_path, driver):
    import_knowledge_to_neo4j(extracted_path, driver)

def Search(Sent_to_ask, driver, gemini_model):
    enhanced_schema, schema_error = get_enhanced_schema(driver)
    user_questions_to_ask = Sent_to_ask
    ask_question(
    user_questions=user_questions_to_ask,
    enhanced_schema=enhanced_schema,
    schema_error=schema_error,
    gemini_model=gemini_model,
    neo4j_driver=driver
        )
        

# Build_base(r"E:\Code\Proj1\data\test.docx", 'data/doc_to_markdown.md', nlp, gemini_model)
# Storge(r"E:\Code\Proj1\extracted_knowledge.json", driver)
Search([ "如果我有异议，我该怎么提出？"], driver, gemini_model)