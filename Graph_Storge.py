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
        session.run("MATCH (n) DETACH DELETE n")
        print("已清除Neo4j中所有现有数据。")

        known_entities = {} # {entity_name: entity_type}

        # 先收集所有实体并创建，避免在关系循环中重复创建事务
        all_nodes_to_create = set() # (node_name, node_type)
        for item in all_extracted_knowledge:
            for entity in item.get('entities', []):
                entity_name = entity['text']
                node_type = entity['label'].replace('GPE', 'Location').replace('LOC', 'Location')
                all_nodes_to_create.add((entity_name, node_type))
                known_entities[entity_name] = node_type # 预填充 known_entities

            # 同样，对于关系的主体和客体，如果它们不是直接抽取的实体，也会被视为 Concept
            for relation in item.get('relations', []):
                subject_name = relation['subject']
                object_name = relation['object']
                if subject_name not in known_entities:
                    # 假定这些非NER抽取的subject/object是Concept
                    all_nodes_to_create.add((subject_name, "Concept"))
                    known_entities[subject_name] = "Concept"
                if object_name not in known_entities:
                    all_nodes_to_create.add((object_name, "Concept"))
                    known_entities[object_name] = "Concept"

        # 批量创建所有节点
        print("正在创建所有节点...")
        for node_name, node_type in tqdm(all_nodes_to_create, desc="创建节点"):
            session.write_transaction(create_node, node_name, node_type)

        # 然后再处理关系
        print("正在创建所有关系...")
        for item in tqdm(all_extracted_knowledge, desc="创建关系"):
            relations_in_sentence = item.get('relations', [])

            for relation in relations_in_sentence: # <-- 这里移除了多余的嵌套循环
                subject_name = relation['subject']
                object_name = relation['object']

                # 对关系类型进行更彻底的清洗和规范化
                relation_type_raw = relation['relation']
                # 1. 替换空格为下划线
                cleaned_relation_type = relation_type_raw.replace(" ", "_")
                # 2. 移除所有非字母、数字、下划线的字符 (保留中文)
                cleaned_relation_type = re.sub(r'[^\w\u4e00-\u9fa5]+', '', cleaned_relation_type)
                # 3. 转换为大写（可选，但推荐保持一致性）
                relation_type = cleaned_relation_type.upper()

                if not relation_type:
                    print(f"警告: 关系类型 '{relation_type_raw}' 清理后为空，跳过此关系。")
                    continue

                # 从已知的实体类型中获取主体和客体的类型
                sub_type = known_entities.get(subject_name, "Concept") # 应该已经在上面预填充
                obj_type = known_entities.get(object_name, "Concept") # 应该已经在上面预填充

                session.write_transaction(
                    create_relationship,
                    subject_name, sub_type,
                    relation_type,
                    object_name, obj_type
                )
    print("\n知识图谱导入完成！")
    # driver.close() # 通常不在这里关闭驱动，因为你可能希望在程序其他地方继续使用

# --- 示例使用 ---
if __name__ == "__main__":
    # 确保 'extracted_knowledge.json' 文件存在
    # 如果你还没有运行过知识抽取步骤，请先运行 step2_knowledge_extraction_gemini.py 来生成此文件
    extracted_knowledge_path = 'extracted_knowledge.json'
    import_knowledge_to_neo4j(extracted_knowledge_path)

    print("\n请打开 Neo4j Browser (通常在 http://localhost:7474/)，用 'neo4j' 用户名和你的密码登录，")
    print("然后尝试运行 Cypher 查询，例如: `MATCH (n)-[r]->(m) RETURN n,r,m LIMIT 50` 来查看导入的图谱。")