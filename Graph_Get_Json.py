
import spacy
import os
import json
from tqdm import tqdm
from docx import Document
import mammoth
import pypandoc
import google.generativeai as genai
import re
import time

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
        if ent.label_ in ["PERSON", "ORG", "GPE", "LOC", "PRODUCT", "EVENT", "DATE", "MONEY", "QUANTITY"]: # 人、组织、地理政治实体、地点、产品、事件
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
    prompt_content = f"""你是一个能够从招投标文本中精确识别实体间关系的AI助手。请从给定的句子和已识别的实体中，识别所有明确的关系三元组（主体，关系，客体）。
句子: {sentence}

已识别的实体: {entity_str}
请特别关注以下在招投标文件中常见的关系类型，并严格按照提供的示例格式进行抽取：
- **项目信息**: (项目, 项目名称是, 名称), (项目, 项目编号是, 编号), (项目, 预算是, 金额), (项目, 服务范围是, 描述)
- **参与方角色**: (项目, 招标人是, 组织), (项目, 招标代理机构是, 组织), (项目, 投标人是, 组织)
- **时间/日期**: (事件/活动, 截止日期是, 日期), (服务, 期限是, 日期范围)
- **财务信息**: (保证金, 金额是, 数值), (支付, 方式是, 描述)
- **要求与条件**: (实体/服务, 要求是, 描述), (主体, 须提供, 材料/资质)
- **位置信息**: (组织/项目, 位于, 地点/行政区划)

请确保关系是清晰、直接且严格基于文本的。

**示例：**
- **句子**: "本招标项目为2024-2027年度重庆联通综合代维服务采购，项目编号：0701-2440CQ010207，招标人为中国联合网络通信有限公司重庆市分公司，招标代理机构为中技国际招标有限公司。"
  **已识别实体**: '2024-2027年度重庆联通综合代维服务采购' (PRODUCT), '0701-2440CQ010207' (PRODUCT), '中国联合网络通信有限公司重庆市分公司' (ORG), '中技国际招标有限公司' (ORG)
  **期望关系**:
    - {{"subject": "本招标项目", "relation": "是", "object": "2024-2027年度重庆联通综合代维服务采购"}}
    - {{"subject": "2024-2027年度重庆联通综合代维服务采购", "relation": "项目编号是", "object": "0701-2440CQ010207"}}
    - {{"subject": "2024-2027年度重庆联通综合代维服务采购", "relation": "招标人是", "object": "中国联合网络通信有限公司重庆市分公司"}}
    - {{"subject": "2024-2027年度重庆联通综合代维服务采购", "relation": "招标代理机构是", "object": "中技国际招标有限公司"}}
- **句子**: "项目资金由招标人自筹，资金已落实。出资比例为100%，项目已具备招标条件。"
  **已识别实体**: '项目资金' (MONEY), '招标人' (PERSON), '100%' (PERCENT)
  **期望关系**:
    - {{"subject": "项目资金", "relation": "由...自筹", "object": "招标人"}}
    - {{"subject": "项目", "relation": "出资比例是", "object": "100%"}}
- **句子**: "本项目为集中招标项目，为重庆联通40个区县分公司和轨道提供综合代维服务，代维主要包含：无线接入网、宽带网、传输及专线的维护工作，三年综合代维费预算总额为31267.26万元（不含增值税）。"
  **已识别实体**: '重庆联通40个区县分公司' (ORG), '轨道' (LOC), '综合代维服务' (PRODUCT), '无线接入网' (PRODUCT), '宽带网' (PRODUCT), '传输' (PRODUCT), '专线' (PRODUCT), '三年' (DATE), '31267.26万元' (MONEY)
  **期望关系**:
    - {{"subject": "本项目", "relation": "提供", "object": "综合代维服务"}}
    - {{"subject": "综合代维服务", "relation": "提供给", "object": "重庆联通40个区县分公司"}}
    - {{"subject": "综合代维服务", "relation": "提供给", "object": "轨道"}}
    - {{"subject": "综合代维服务", "relation": "包含", "object": "无线接入网的维护工作"}}
    - {{"subject": "综合代维服务", "relation": "包含", "object": "宽带网的维护工作"}}
    - {{"subject": "综合代维服务", "relation": "包含", "object": "传输及专线的维护工作"}}
    - {{"subject": "三年综合代维费", "relation": "预算总额为", "object": "31267.26万元"}}

请严格按照 JSON 数组格式返回结果。每个元素都是一个字典，包含 "subject", "relation", "object" 字段。
如果未找到任何关系，返回空数组 []。
不要包含任何额外文字或解释，只返回 JSON 数组。
"""
    time.sleep(0.3)
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


def docx_to_markdown_mammoth(docx_path, md_path):
    with open(docx_path, "rb") as docx_file:
        # mammoth.convert_to_markdown 返回一个 ConversionResult 对象
        result = mammoth.convert_to_markdown(docx_file)
        markdown_text = result.value # 转换后的 Markdown 文本
        # messages = result.messages # 转换过程中可能产生的警告或错误信息

    with open(md_path, "w", encoding="utf-8") as md_file:
        md_file.write(markdown_text)
    print(f"'{docx_path}' 成功转换为 '{md_path}'。")
    return markdown_text


def docx_to_markdown(docx_path: str, output_md_path: str) -> str | None:
    """
    将指定的 .docx 文件转换为 Markdown 格式。

    该函数会利用 pandoc 工具进行高质量的格式转换，特别适合处理包含
    表格、列表等复杂结构的文档。

    Args:
        docx_path (str): 输入的 Word 文档 (.docx) 的文件路径。
        output_md_path (str): 输出的 Markdown (.md) 文件的保存路径。

    Returns:
        str | None: 如果转换成功，则返回转换后的 Markdown 文本字符串，
                    并将其保存到 output_md_path。
                    如果转换失败（例如 pandoc 未安装或文件不存在），
                    则返回 None。
    """
    # 确保输入文件存在
    if not os.path.exists(docx_path):
        print(f"[错误] 输入文件不存在: {docx_path}")
        return None

    # 自动创建输出文件所在的目录
    output_dir = os.path.dirname(output_md_path)
    if output_dir: # 确保路径中包含目录部分
        os.makedirs(output_dir, exist_ok=True)

    try:
        print(f"开始转换: {docx_path} -> {output_md_path}")
        
        # 1. 调用 pypandoc 将文件转换为字符串
        # 'gfm' 代表 GitHub Flavored Markdown，是目前最通用的 Markdown 格式，对表格支持很好
        # extra_args 可以用来传递 Pandoc 的命令行参数，例如 --wrap=none 可以防止自动换行
        markdown_string = pypandoc.convert_file(
            docx_path, 
            'gfm', 
            extra_args=['--wrap=none']
        )
        
        # 2. 将转换后的字符串写入文件
        with open(output_md_path, "w", encoding="utf-8") as f:
            f.write(markdown_string)
            
        print(f"文档已成功转换为 Markdown 并保存。")
        
        # 3. 返回转换后的字符串
        return markdown_string

    except Exception as e:
        print(f"使用 pypandoc 转换失败: {e}")
        print("请确保您已经正确安装了 Pandoc 并且它在系统的 PATH 中。")
        return None
    

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

