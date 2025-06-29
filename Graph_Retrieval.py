# pip install neo4j google-generativeai # 确保安装了Neo4j驱动和Gemini库

import os
import json
from neo4j import GraphDatabase
import google.generativeai as genai


def get_detailed_neo4j_schema(driver, max_retries=3):
    """
    使用 APOC 获取详细的 Neo4j 图谱 Schema，并格式化为文本。
    这为 LLM 提供了节点属性和关系连接的上下文。
    """
    print("正在获取详细的图谱 Schema (需要 APOC 插件)...")
    schema_text = ""
    query = "CALL apoc.meta.schema()"

    try:
        with driver.session() as session:
            result = session.run(query)
            schema_data = result.single()[0]

            # 1. 格式化节点及其属性
            schema_text += "## 节点标签和属性:\n"
            node_labels = sorted([k for k, v in schema_data.items() if v['type'] == 'node'])
            for label in node_labels:
                details = schema_data[label]
                properties = ", ".join([f"`{prop}` ({details['properties'][prop]['type']})" for prop in sorted(details['properties'])])
                schema_text += f"- 节点 `:{label}` 的属性: {properties}\n"

            # 2. 格式化关系及其属性和连接
            schema_text += "\n## 关系类型、属性和连接:\n"
            rel_types = sorted([k for k, v in schema_data.items() if v['type'] == 'relationship'])
            for rel_type in rel_types:
                details = schema_data[rel_type]
                properties = ""
                if 'properties' in details and details['properties']:
                    prop_list = ", ".join([f"`{prop}` ({details['properties'][prop]['type']})" for prop in sorted(details['properties'])])
                    properties = f" (属性: {prop_list})"

                # 使用更高效的方式推断关系连接
                connections = []
                for conn in details.get('connections', []):
                    start_node = conn['start']['label']
                    end_node = conn['end']['label']
                    connections.append(f"(:{start_node})-[:{rel_type}]->(:{end_node})")
                
                if connections:
                    unique_connections = " | ".join(sorted(list(set(connections))))
                    schema_text += f"- 关系 `[:{rel_type}]`{properties} 连接: {unique_connections}\n"
                else:
                    schema_text += f"- 关系 `[:{rel_type}]`{properties} (连接模式未在元数据中明确定义)\n"

            return schema_text, None

    except Exception as e:
        if "Unknown function 'apoc.meta.schema'" in str(e):
            error_msg = "获取详细Schema失败: APOC 插件未安装或未正确配置。请在 Neo4j 中安装 APOC 插件以获得最佳性能。"
            print(f"错误: {error_msg}")
            return None, error_msg
        
        print(f"执行 `apoc.meta.schema` 失败: {e}")
        return None, f"无法获取Schema: {e}"


def generate_cypher_query(user_question, detailed_schema_text, llm_model):
    """
    使用LLM根据用户问题和详细的图谱Schema生成Cypher查询。
    """
    if not llm_model:
        return "MATCH (n) RETURN n.name LIMIT 5", "LLM未加载，返回默认查询。"
    
    if not detailed_schema_text:
         return "MATCH (n) RETURN n.name LIMIT 5", "Schema信息缺失，返回默认查询。"

    prompt_content = f"""你是一个顶级的Neo4j图数据库专家，专门将自然语言问题转换成精确的Cypher查询语句。

## 你的任务
根据下面提供的图谱Schema信息，将用户提出的问题转换成一个**可执行的、高效的**Cypher查询语句。

---
## Neo4j 图谱 Schema
{detailed_schema_text}
---

## 查询生成规则 (必须严格遵守):
1.  **完全基于Schema**: 你的查询必须严格依据上面提供的节点标签、关系类型和属性。**绝对不允许**虚构或猜测任何Schema中未列出的标签、关系或属性。
2.  **仅返回Cypher**: 你的回答**必须且只能**是一句Cypher查询语句。不要包含任何解释、注释、代码块标记（如 ```cypher）或其他任何文字。
3.  **属性匹配**: 当问题涉及具体实体时（如“李华”），使用 `{{name: '李华'}}` 进行精确属性匹配。假设`name`是主要的标识属性。
4.  **处理模糊问题**: 如果问题意图是探索性的（例如“有效期至包括什么？”），请查询与该关系或节点相关的实体类型或名称。例如，可以查询该关系连接的节点的标签或名称：`MATCH ()-[:有效期至]->(m) RETURN DISTINCT labels(m), m.name LIMIT 10`。
5.  **添加限制**: 为了防止返回过多结果，请在查询末尾使用 `LIMIT`，例如 `LIMIT 25`。
6.  **无法回答**: 如果根据提供的Schema，问题无法被回答，或者问题与图谱无关，请**只返回**一个特定的单词：`CANNOT_ANSWER`。

## 示例:
- 用户问题: "清华大学的教授都有谁？"
- Schema 中有: `(:Person)-[:任职于]->(:ORG)` 且 `Person` 有 `title: '教授'` 和 `name` 属性
- Cypher 查询: `MATCH (p:Person {{title: '教授'}})-[:任职于]->(o:ORG {{name: '清华大学'}}) RETURN p.name`

---
## 用户问题:
"{user_question}"

---
## Cypher 查询:
"""
    try:
        response = llm_model.generate_content(
            prompt_content,
            generation_config=genai.types.GenerationConfig(temperature=0.0)
        )
        cypher_query = response.text.strip().replace("```cypher", "").replace("```", "").strip()
        
        if not cypher_query:
             return "CANNOT_ANSWER", "LLM 返回了空查询。"

        return cypher_query, None
    except Exception as e:
        print(f"Error generating Cypher query with LLM: {e}")
        return "", f"LLM查询生成失败: {e}"


def execute_cypher_query(driver, query):
    """ 在Neo4j中执行Cypher查询并返回结果。 """
    if not driver:
        return [], "Neo4j 驱动未初始化。"
    try:
        with driver.session() as session:
            result = session.run(query)
            # 将结果转换为字典列表，更易于处理
            return [record.data() for record in result], None
    except Exception as e:
        return [], f"执行Cypher查询失败: {e}"


def format_results_for_display(records):
    """ 将Neo4j查询结果格式化为可读的字符串。 """
    if not records:
        return "未从知识图谱中找到相关信息。"
    # 使用json.dumps美化输出，确保中文正常显示
    return json.dumps(records, indent=2, ensure_ascii=False)

def ask_question(user_questions, detailed_schema, schema_error, gemini_model, neo4j_driver):
    if schema_error:
        print(f"\n无法继续，因为: {schema_error}")
        neo4j_driver.close()
        exit()
    print("\n--- 详细图谱 Schema ---")
    print(detailed_schema)
    
    print("\n--- 开始进行图谱检索 ---")
    for question in user_questions:
        print(f"\n{'='*20}\n用户问题: {question}")
        
        # 3. 生成Cypher查询
        print("-> 正在生成Cypher查询...")
        cypher_query, query_gen_error = generate_cypher_query(question, detailed_schema, gemini_model)
        
        if query_gen_error:
            print(f"   [!] 查询生成失败: {query_gen_error}")
            continue
        
        if cypher_query == "CANNOT_ANSWER":
            print("   [i] LLM判断该问题无法通过图谱回答。")
            continue

        print(f"   [OK] 生成的Cypher查询: {cypher_query}")
        
        # 4. 执行Cypher查询
        print("-> 正在执行Cypher查询...")
        retrieved_records, exec_error = execute_cypher_query(neo4j_driver, cypher_query)
        
        if exec_error:
            print(f"   [!] Cypher查询执行失败: {exec_error}")
        else:
            print(f"   [OK] 检索到 {len(retrieved_records)} 条记录。")
            formatted_context = format_results_for_display(retrieved_records)
            print(f"   [结果]:\n{formatted_context}")
            
    neo4j_driver.close()
    print("\n完成所有查询。")

# --- 主执行逻辑 ---
# if __name__ == "__main__":
#     if not neo4j_driver or not gemini_model:
#         print("数据库或LLM未初始化，程序退出。")
#         if neo4j_driver:
#             neo4j_driver.close()
#         exit()

#     # 1. 获取详细的图谱Schema
#     detailed_schema, schema_error = get_detailed_neo4j_schema(neo4j_driver)

#     # 2. 用户问题示例
#     user_questions = [
#         "宽带网维护包括什么"
#     ]
    
   