
import spacy
import os
import json
from tqdm import tqdm
from docx import Document
import re
# --- 配置 Gemini LLM ---


def perform_ner(sentence, nlp):
    """
    使用spaCy进行命名实体识别。
    返回一个字典列表，每个字典包含实体文本和标签。
    """
    if nlp is None:
        return []
    doc = nlp(sentence)
    entities = []
    for ent in doc.ents:
        # 过滤掉一些通用但可能不那么重要的实体类型，例如日期、数量
        # 根据你的文档类型和需求调整过滤规则
        if ent.label_ in ["PERSON", "ORG", "GPE", "LOC", "PRODUCT", "EVENT"]: # 人、组织、地理政治实体、地点、产品、事件
            entities.append({"text": ent.text, "label": ent.label_})
    return entities

def extract_relations_with_llm_gemini(sentence, identified_entities, gemini_model_client):
    """
    使用Gemini LLM从句子和已识别实体中抽取关系三元组。
    """
    if not gemini_model_client:
        return []

    if not identified_entities:
        return []

    # 将实体格式化为LLM友好的字符串
    entity_str = ", ".join([f"'{e['text']}' ({e['label']})" for e in identified_entities])

    # 设计Prompt来引导LLM抽取关系。这是关键！
    # 示例中的Prompt是为通用LLM设计的，对于Gemini同样适用
    prompt_content = f"""你是一个能够从文本中精确识别实体间关系的AI助手。请从给定的句子和实体中，识别所有明确的关系三元组（主体，关系，客体）。请确保关系是清晰、直接且基于文本的。

句子: {sentence}

已识别的实体: {entity_str}

请严格按照 JSON 数组格式返回结果。每个元素都是一个字典，包含 "subject", "relation", "object" 字段。
例如：
[
  {{"subject": "李华", "relation": "就职于", "object": "清华大学"}},
  {{"subject": "清华大学", "relation": "位于", "object": "北京"}}
]
如果未找到任何关系，返回空数组 []。
不要包含任何额外文字或解释，只返回 JSON 数组。
"""

    try:
        # 调用Gemini API
        response = gemini_model_client.generate_content(
            prompt_content,
            generation_config=genai.types.GenerationConfig(
                temperature=0.0, # 降低温度以获得更确定的结果
                response_mime_type="application/json" # 明确要求返回JSON格式，Gemini特有
            )
        )
        
        # Gemini的响应通常在 text 属性中，且如果指定了JSON格式，则直接是JSON字符串
        content = response.text
        return json.loads(content)
    except json.JSONDecodeError:
        print(f"Error decoding JSON from Gemini LLM response for sentence: {sentence}\nResponse: {content}")
        return []
    except Exception as e:
        print(f"Error calling Gemini LLM for relation extraction: {e}")
        return []


def docx_to_txt(docx_path, txt_path):
    if not os.path.exists(docx_path):
        print('ERROR, NO DOC PATH')
    try:
        document = Document(docx_path)
        full_text = []
        for para in document.paragraphs:
            full_text.append(para.text)
        
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(full_text))
        
        print(f"成功将 '{docx_path}' 转换为 '{txt_path}'")
        return True
    except Exception as e:
        print(f"转换 '{docx_path}' 时发生错误：{e}")
        return False
    
def load_text_file(txt_path):
    try:
        with open(txt_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        print(f"Error reading TXT {txt_path}: {e}")
        return ""

def preprocess_text(text):
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'\{.*?\}', '', text)
    text = re.sub(r'\(.*?\)', '', text)

    sentences = re.split(r'([。？！；?!;])', text)
    processed_sentences = []
    for i in range(0, len(sentences) - 1, 2):
        sentence = sentences[i].strip()
        delimiter = sentences[i+1].strip() if i+1 < len(sentences) else ''
        if sentence:
            processed_sentences.append(sentence + delimiter)
    if len(sentences) % 2 == 1 and sentences[-1].strip():
        processed_sentences.append(sentences[-1].strip())
    return processed_sentences

def process_and_save(sentences, nlp, gemini_model):
    all_extracted_knowledge = [] # 存储所有的实体和关系

    if nlp: # 只需要NER模型加载成功即可开始处理
        print("\n--- 开始知识抽取 (NER 和 LLM辅助 RE) ---")
        # 检查Gemini模型是否初始化成功
        if not gemini_model:
            print("警告: Gemini模型未成功加载，将无法进行关系抽取。请检查您的 GOOGLE_API_KEY。")

        for i, sentence in tqdm(enumerate(sentences), total=len(sentences), desc="抽取知识"):
            entities = perform_ner(sentence, nlp)
            relations = []
            if gemini_model: # 只有当Gemini模型可用时才尝试进行关系抽取
                relations = extract_relations_with_llm_gemini(sentence, entities, gemini_model)
            
            if entities or relations:
                all_extracted_knowledge.append({
                    "sentence": sentence,
                    "entities": entities,
                    "relations": relations
                })
        
        print("\n--- 抽取结果示例（前5个有知识的句子） ---")
        for item in all_extracted_knowledge[:5]:
            print(f"句子: {item['sentence']}")
            print(f"  实体: {item['entities']}")
            print(f"  关系: {item['relations']}")
            print("-" * 20)

        # 保存抽取结果到JSON文件
        with open('extracted_knowledge.json', 'w', encoding='utf-8') as f:
            json.dump(all_extracted_knowledge, f, ensure_ascii=False, indent=4)
        print("\n所有抽取到的知识已保存到 'extracted_knowledge.json'")

    else:
        print("由于spaCy模型加载失败，跳过知识抽取。请检查下载。")

# 加载中文模型，建议在程序开始时加载一次即可，避免重复加载

def preprocess_text_optimized_v2(text, nlp):
    """功能层面的优化版本，使用spaCy"""
    # 1. 同样先做一些基础清理，但对于括号内容可以选择保留
    text = re.sub(r'\s+', ' ', text).strip()
    
    # 2. 使用spaCy处理文本
    doc = nlp(text)
    
    # 3. doc.sents 会生成一个包含准确切分后句子的迭代器
    # 我们将每个句子对象转换回文本
    return [sent.text.strip() for sent in doc.sents if sent.text.strip()]
# --- 备用：requests 模拟 LLM API 调用 (注释掉作为备用) ---
# import requests # 导入requests库
# def extract_relations_with_llm_requests(sentence, identified_entities):
#     """
#     使用requests库向LLM API发送请求，从句子和已识别实体中抽取关系三元组。
#     这是备用方案，当前使用Gemini API。
#     """
#     # LLM_API_URL = "https://api.openai.com/v1/chat/completions" # 替换为你的LLM服务地址
#     # LLM_API_KEY = os.getenv("OPENAI_API_KEY") # 假设API Key仍在环境变量中

#     # if not LLM_API_KEY:
#     #     print("Error: LLM_API_KEY is not set. Cannot call LLM API.")
#     #     return []

#     # if not identified_entities:
#     #     return []

#     # entity_str = ", ".join([f"'{e['text']}' ({e['label']})" for e in identified_entities])
#     # prompt_messages = [...] # 同上文的 prompt_messages

#     # headers = {
#     #     "Content-Type": "application/json",
#     #     "Authorization": f"Bearer {LLM_API_KEY}"
#     # }

#     # payload = {
#     #     "model": "gpt-3.5-turbo",
#     #     "messages": prompt_messages,
#     #     "temperature": 0,
#     #     "response_format": {"type": "json_object"}
#     # }

#     # try:
#     #     response = requests.post(LLM_API_URL, headers=headers, json=payload, timeout=60)
#     #     response.raise_for_status()
#     #     response_data = response.json()
#     #     content = response_data['choices'][0]['message']['content']
#     #     return json.loads(content)
#     # except requests.exceptions.RequestException as e:
#     #     print(f"Error calling LLM API (network/HTTP issue): {e}")
#     #     return []
#     # except json.JSONDecodeError:
#     #     print(f"Error decoding JSON from LLM response for sentence: {sentence}\nResponse content: {content}")
#     #     return []
#     # except KeyError as e:
#     #     print(f"Error parsing LLM response structure: Missing key {e}. Response: {response_data}")
#     #     return []
#     # except Exception as e:
#     #     print(f"An unexpected error occurred: {e}")
#     #     return []
#     pass # 保持函数存在但无实际执行

# --- 示例使用 ---
if __name__ == "__main__":
    # 为了让这个脚本独立运行，我们在这里定义简化的 step1_preprocess 函数
    import re
    
    # 确保 data 目录存在并创建示例文件
    if not os.path.exists('data'):
        os.makedirs('data')
    sample_txt_path = 'data/sample_document.txt'
    docx_to_txt(r"E:\Code\Proj1\data\2024-2027年度重庆联通综合代维服务采购.docx", sample_txt_path)
    # with open(sample_txt_path, 'w', encoding='utf-8') as f:
    #     f.write("LangChain是一个强大的框架，用于开发由语言模型驱动的应用程序。它提供了一系列模块，可以帮助您构建检索增强生成（RAG）系统、智能体等等。\n")
    #     f.write("李华是清华大学的教授。他在人工智能领域有很深的造诣。清华大学位于中国北京。")


    document_content = load_text_file(sample_txt_path)
    processed_sentences = preprocess_text(document_content)

