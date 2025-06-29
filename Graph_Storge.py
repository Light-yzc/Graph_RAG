import json
import os
from neo4j import GraphDatabase
from tqdm import tqdm
import re


def create_node(tx, node_name, node_type):
    """
    在 Neo4j 中创建或合并一个节点。
    MERGE 会在节点不存在时创建，存在时匹配。
    节点会带有 $node_type 标签和 name 属性。
    """
    query = (
        f"MERGE (n:{node_type} {{name: $node_name}})"
        "RETURN n"
    )
    tx.run(query, node_name=node_name)

def create_relationship(tx, subject_name, subject_type, relation_type, object_name, object_type):
    """
    在 Neo4j 中创建或合并一个关系。
    确保主体和客体节点存在，然后创建连接它们的关系。
    """
    # Cypher 查询会先确保两个节点存在，然后创建它们之间的关系
    # 这里的 MERGE (a:...) 和 MERGE (b:...) 确保了节点的存在
    # 然后 MERGE (a)-[:...]-(b) 创建了关系
    query = (
        f"MERGE (a:{subject_type} {{name: $subject_name}}) "
        f"MERGE (b:{object_type} {{name: $object_name}}) "
        f"MERGE (a)-[:{relation_type}]->(b)" # 注意这里是单向关系 ->，如果需要双向请调整
    )
    tx.run(query, subject_name=subject_name, object_name=object_name)

def import_knowledge_to_neo4j(extracted_knowledge_file, driver):
    """
    从 JSON 文件加载抽取到的知识（实体和关系），并将其导入 Neo4j。
    """
    if not driver:
        print("Neo4j 驱动未初始化，跳过导入。")
        return

    try:
        with open(extracted_knowledge_file, 'r', encoding='utf-8') as f:
            all_extracted_knowledge = json.load(f)
    except FileNotFoundError:
        print(f"错误: 文件 '{extracted_knowledge_file}' 未找到。请确保知识抽取步骤已完成并生成了该文件。")
        return
    except json.JSONDecodeError:
        print(f"错误: 文件 '{extracted_knowledge_file}' 不是有效的JSON格式。")
        return

    print(f"\n--- 开始导入 {len(all_extracted_knowledge)} 条知识记录到 Neo4j ---")
    with driver.session() as session:
        # **可选操作：清除现有数据 (仅在开发/测试时使用，生产环境慎用！)**
        # 如果你每次都想重新开始一个空的图谱，可以取消注释下面这行。
        # session.run("MATCH (n) DETACH DELETE n")
        # print("已清除Neo4j中所有现有数据。")

        # 为了避免重复处理，我们用一个集合来跟踪已经处理过的实体名称，并存储其类型
        known_entities = {} # {entity_name: entity_type}

        for item in tqdm(all_extracted_knowledge, desc="导入知识到Neo4j"):
            entities_in_sentence = item.get('entities', [])
            relations_in_sentence = item.get('relations', [])

            # 1. 处理本句中的所有实体
            for entity in entities_in_sentence:
                entity_name = entity['text']
                # 规范化实体类型，例如将地理政治实体(GPE)和地点(LOC)都映射为 Location
                node_type = entity['label'].replace('GPE', 'Location').replace('LOC', 'Location')
                
                if entity_name not in known_entities:
                    known_entities[entity_name] = node_type
                
                session.write_transaction(create_node, entity_name, node_type)
            
            # 2. 处理本句中的所有关系
            for relation in relations_in_sentence:
                subject_name = relation['subject']
                object_name = relation['object']
                # 关系类型通常不含空格，统一替换为空格
                relation_type = relation['relation'].replace(" ", "_").upper() # 大写通常是关系的惯例
                for relation in relations_in_sentence:
                    subject_name = relation['subject']
                    object_name = relation['object']
                    
                    # --- 这里是修改的关键部分 ---
                    # 对关系类型进行更彻底的清洗和规范化
                    relation_type_raw = relation['relation']
                    # 1. 替换空格为下划线
                    cleaned_relation_type = relation_type_raw.replace(" ", "_")
                    # 2. 移除所有非字母、数字、下划线的字符
                    #    这个正则表达式会保留汉字、英文字母、数字和下划线
                    cleaned_relation_type = re.sub(r'[^\w\u4e00-\u9fa5]+', '', cleaned_relation_type) # \u4e00-\u9fa5 是中文Unicode范围
                    # 3. 转换为大写（可选，但推荐保持一致性）
                    relation_type = cleaned_relation_type.upper() 
                    
                    # 确保关系类型不是空的，如果清理后变空，可能需要给个默认值或跳过
                    if not relation_type:
                        print(f"警告: 关系类型 '{relation_type_raw}' 清理后为空，跳过此关系。")
                        continue
                # 尝试从已知实体中获取主体和客体的类型
                # 如果LLM抽取的实体不在NER列表中，这里会默认为 'Concept'
                sub_type = known_entities.get(subject_name, "Concept")
                obj_type = known_entities.get(object_name, "Concept")

                # 如果实体是全新的（LLM抽取但NER未识别），也需要创建其节点
                if subject_name not in known_entities:
                     session.write_transaction(create_node, subject_name, sub_type)
                     known_entities[subject_name] = sub_type
                if object_name not in known_entities:
                     session.write_transaction(create_node, object_name, obj_type)
                     known_entities[object_name] = obj_type

                session.write_transaction(
                    create_relationship,
                    subject_name, sub_type,
                    relation_type,
                    object_name, obj_type
                )
    print("\n知识图谱导入完成！")
    driver.close() # 关闭 Neo4j 驱动连接

# --- 示例使用 ---
if __name__ == "__main__":
    # 确保 'extracted_knowledge.json' 文件存在
    # 如果你还没有运行过知识抽取步骤，请先运行 step2_knowledge_extraction_gemini.py 来生成此文件
    extracted_knowledge_path = 'extracted_knowledge.json'
    import_knowledge_to_neo4j(extracted_knowledge_path)

    print("\n请打开 Neo4j Browser (通常在 http://localhost:7474/)，用 'neo4j' 用户名和你的密码登录，")
    print("然后尝试运行 Cypher 查询，例如: `MATCH (n)-[r]->(m) RETURN n,r,m LIMIT 50` 来查看导入的图谱。")