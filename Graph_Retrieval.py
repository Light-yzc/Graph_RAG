import os
import json
from neo4j import GraphDatabase
import google.generativeai as genai

def get_enhanced_schema(driver, sample_limit=3, properties_to_sample=5):
    """
    获取增强的 Neo4j 图谱 Schema，包括：
    1. 基础 Schema (节点, 关系, 属性) - via apoc.meta.schema
    2. 关键属性的数据样本和分布 - via custom queries
    3. 所有类型的索引信息 (B-Tree, Full-text, Vector) - via SHOW INDEXES
    这为 LLM 提供了极其丰富的上下文。
    """
    print("正在获取增强的图谱 Schema (需要 APOC 插件)...")
    schema_text = ""
    relationship_details_override = None
    # ---------------------------------------------------------------
    # 新增：手动定义关系语义和连接模式的映射
    # 这可以在 apoc.meta.schema 无法提供时作为补充，或提供更友好的语义说明
    # 请根据你的实际图谱结构和业务含义来填充此字典
    # ---------------------------------------------------------------
    relationship_details_override = {
        "上传方是": {
            "meaning": "表示一个实体被另一个实体上传，方向从被上传物指向上传者。",
            "pattern": "(:Concept)-[:上传方是]->(:ORG)" # 假设电子投标文件是Concept，上传方是ORG
        },
        "名称是": {
            "meaning": "表示一个实体拥有某个名称。",
            "pattern": "(:Entity)-[:名称是]->(:Concept)" # Entity是泛指，具体取决于你的节点标签
        },
        "位于": {
            "meaning": "表示一个实体位于某个地点。",
            "pattern": "(:Entity)-[:位于]->(:Location)"
        },
        # 根据你的 extracted_knowledge.json 和实际图谱继续添加
        "联系人是": {
            "meaning": "表示一个实体（如招标人）的联系人是某人。",
            "pattern": "(:Entity)-[:联系人是]->(:PERSON)" 
        },
        "联系电话是": {
            "meaning": "表示一个实体的联系电话。",
            "pattern": "(:Entity)-[:联系电话是]->(:Concept)" # 电话号码可能作为Concept或其他类型
        },
        "发布年份是": {
            "meaning": "表示某项内容在指定年份发布。",
            "pattern": "(:Concept)-[:发布年份是]->(:DATE)"
        },
        "发生时间是": {
            "meaning": "表示某事件的发生时间。",
            "pattern": "(:Concept)-[:发生时间是]->(:DATE)"
        },
        "发起渠道是": {
            "meaning": "表示某项活动的来源或渠道。",
            "pattern": "(:Concept)-[:发起渠道是]->(:ORG)"
        },
        "开标方式是": {
            "meaning": "表示招标的开标方式。",
            "pattern": "(:Concept)-[:开标方式是]->(:Concept)"
        },
        "数量是": {
            "meaning": "表示实体的数量信息。",
            "pattern": "(:Concept)-[:数量是]->(:Concept)"
        },
        "是": {
            "meaning": "表示一种身份、定义或等同关系。",
            "pattern": "(:Entity)-[:是]->(:Concept)"
        },
        "有权否决": {
            "meaning": "表示一个实体有权否决另一个实体。",
            "pattern": "(:Entity)-[:有权否决]->(:Entity)"
        },
        "期限是": {
            "meaning": "表示某项事物的有效期限或截止日期。",
            "pattern": "(:Concept)-[:期限是]->(:DATE)"
        },
        "由确认": {
            "meaning": "表示某个实体由另一个实体确认。",
            "pattern": "(:Concept)-[:由确认]->(:Entity)"
        },
        "累计得": {
            "meaning": "表示累计获得某个数量或成果。",
            "pattern": "(:Entity)-[:累计得]->(:Concept)"
        },
        "要求": {
            "meaning": "表示一个实体对另一个实体提出要求。",
            "pattern": "(:Entity)-[:要求]->(:Concept)"
        },
        "要求是": {
            "meaning": "表示提出的具体要求内容。",
            "pattern": "(:Concept)-[:要求是]->(:Concept)"
        },
        "通过": {
            "meaning": "表示通过某种方式或渠道。",
            "pattern": "(:Concept)-[:通过]->(:Concept)"
        },
        "金额是": {
            "meaning": "表示涉及的金额信息。",
            "pattern": "(:Concept)-[:金额是]->(:MONEY)"
        },
        "须注明": {
            "meaning": "表示需要注明的事项。",
            "pattern": "(:Concept)-[:须注明]->(:Concept)"
        },
         "在进行": {
            "meaning": "表示某种活动或事件正在进行。",
            "pattern": "(:Concept)-[:在进行]->(:Concept)"
        },
        # ... 其他关系类型，按需添加
    }
    # ---------------------------------------------------------------

    try:
        with driver.session() as session:
            # 1. 使用 APOC 获取基础 Schema
            print("  - 步骤 1/3: 获取基础节点和关系 Schema...")
            meta_schema_result = session.run("CALL apoc.meta.schema()")
            schema_data = meta_schema_result.single()[0]

            # 2. 格式化节点、属性，并抓取数据样本
            schema_text += "## 节点标签、属性和数据样本:\n"
            node_labels = sorted([k for k, v in schema_data.items() if v['type'] == 'node'])
            
            for label in node_labels:
                details = schema_data[label]
                prop_list = []
                
                # 只对部分字符串属性进行采样，避免性能问题
                string_props = [
                    prop for prop, detail in details.get('properties', {}).items() 
                    if detail.get('type') == 'STRING'
                ]
                props_to_process = sorted(string_props)[:properties_to_sample]

                for prop in sorted(details['properties']):
                    prop_type = details['properties'][prop]['type']
                    prop_str = f"`{prop}` ({prop_type})"
                    
                    # 为部分字符串属性获取样本
                    if prop in props_to_process:
                        try:
                            sample_query = f"""
                            MATCH (n:{label}) 
                            WHERE n.`{prop}` IS NOT NULL
                            WITH n.`{prop}` AS value, count(*) AS count
                            RETURN value, count 
                            ORDER BY count DESC LIMIT {sample_limit}
                            """
                            sample_result = session.run(sample_query)
                            samples = [f"'{rec['value']}' ({rec['count']})" for rec in sample_result]
                            if samples:
                                prop_str += f" (例如: {', '.join(samples)})"
                        except Exception as e:
                            prop_str += " (采样失败)"

                    prop_list.append(prop_str)
                
                schema_text += f"- **:{label}**\n  - 属性: {'; '.join(prop_list)}\n"

            # --- 关键修改：先收集所有关系的连接模式 ---
            # 这个字典将存储每个关系类型的所有发现的连接模式
            # 例如: {'上传方是': ['(:Concept)-[:上传方是]->(:ORG)']}
            all_rel_connections = {} 

            for node_label in node_labels:
                node_details = schema_data[node_label]
                if 'relationships' in node_details:
                    for rel_type, rel_detail in node_details['relationships'].items():
                        # 只关注出站关系，避免重复（因为入站关系就是某个节点的出站关系）
                        if rel_detail['direction'] == 'out':
                            start_node = node_label
                            # apoc.meta.schema() 的 labels 列表表示目标节点类型
                            for end_node in rel_detail.get('labels', []): 
                                pattern = f"(:{start_node})-[:{rel_type}]->(:{end_node})"
                                if rel_type not in all_rel_connections:
                                    all_rel_connections[rel_type] = set()
                                all_rel_connections[rel_type].add(pattern)

            # 3. 格式化关系及其属性和连接模式
            schema_text += "\n## 关系类型、属性和连接:\n"
            rel_types_in_schema = sorted([k for k, v in schema_data.items() if v['type'] == 'relationship'])
            for rel_type in rel_types_in_schema:
                details = schema_data[rel_type] # 这里的 details 依然是顶层关系对象，用于获取属性
                properties = ""
                if 'properties' in details and details['properties']:
                    prop_list = [f"`{prop}` ({details['properties'][prop]['type']})" for prop in sorted(details['properties'])]
                    properties = f" (属性: {', '.join(prop_list)})"

                # 优先使用手动定义的连接模式和含义
                if relationship_details_override:
                    override_info = relationship_details_override.get(rel_type)
                else:    
                    override_info = None
                
                if override_info:
                    meaning_str = f"  - 含义: {override_info['meaning']}\n" if override_info.get('meaning') else ""
                    pattern_str = f"  - 连接模式: {override_info['pattern']}\n" if override_info.get('pattern') else ""
                    schema_text += f"- **[:{rel_type}]**{properties}\n{meaning_str}{pattern_str}"
                else:
                    # 如果没有手动定义，则尝试从 all_rel_connections 中获取
                    found_connections = all_rel_connections.get(rel_type)
                    if found_connections:
                        unique_connections = " | ".join(sorted(list(found_connections)))
                        schema_text += f"- **[:{rel_type}]**{properties}\n  - 连接模式: {unique_connections}\n"
                    else:
                        # 如果 apoc.meta.schema 和手动定义都没有提供，则显示未定义
                        schema_text += f"- **[:{rel_type}]**{properties} (连接模式未在元数据中明确定义)\n"

            # 4. 获取并格式化所有索引信息
            print("  - 步骤 2/3: 获取索引信息...")
            schema_text += "\n## 可用索引信息:\n"
            indexes_result = session.run("SHOW INDEXES")
            indexes = list(indexes_result) # Consume the result
            
            # B-Tree 和 Range 索引
            btree_indexes = [
                idx for idx in indexes 
                if idx['type'] == 'RANGE' or idx['type'] == 'BTREE'
            ]
            if btree_indexes:
                schema_text += "- **B-Tree/Range 索引 (用于精确匹配和范围查询):**\n"
                for idx in btree_indexes:
                    labels = idx['labelsOrTypes'][0] if idx['labelsOrTypes'] else ''
                    properties = ", ".join(idx['properties'])
                    schema_text += f"  - `:{labels}({properties})` (名称: {idx['name']})\n"
            
            # 全文索引
            fulltext_indexes = [idx for idx in indexes if idx['type'] == 'FULLTEXT']
            if fulltext_indexes:
                schema_text += "- **全文索引 (用于关键词搜索):**\n"
                for idx in fulltext_indexes:
                    label = idx['labelsOrTypes'][0] if idx['labelsOrTypes'] else ''
                    properties = ", ".join(idx['properties'])
                    schema_text += f"  - `:{label}({properties})` (名称: {idx['name']}) -> 使用 `CALL db.index.fulltext.queryNodes('{idx['name']}', '关键词')`\n"

            # 向量索引
            vector_indexes = [idx for idx in indexes if idx['type'] == 'VECTOR']
            if vector_indexes:
                schema_text += "- **向量索引 (用于语义相似度搜索):**\n"
                for idx in vector_indexes:
                    label = idx['labelsOrTypes'][0] if idx['labelsOrTypes'] else ''
                    prop = idx['properties'][0] if idx['properties'] else ''
                    schema_text += f"  - `:{label}({prop})` (名称: {idx['name']}) -> 使用 `CALL db.index.vector.queryNodes(...)`\n"
            
            if not btree_indexes and not fulltext_indexes and not vector_indexes:
                schema_text += "- 无可用索引。\n"

            print("  - 步骤 3/3: Schema 信息构建完成。")
            return schema_text, None

    except Exception as e:
        if "Unknown function 'apoc.meta.schema'" in str(e):
            error_msg = "获取详细Schema失败: APOC 插件未安装或未正确配置。"
            print(f"错误: {error_msg}")
            return None, error_msg
        
        print(f"执行 Schema 获取失败: {e}")
        return None, f"无法获取Schema: {e}"


# --- 优化的查询生成函数 ---
def generate_cypher_query(user_question, enhanced_schema_text, llm_model, prompt=None):
    """
    使用LLM根据用户问题和增强的图谱Schema生成Cypher查询。
    """
    if not llm_model:
        return "MATCH (n) RETURN n.name LIMIT 5", "LLM未加载，返回默认查询。"
    
    if not enhanced_schema_text:
        return "MATCH (n) RETURN n.name LIMIT 5", "Schema信息缺失，返回默认查询。"

    # --- 这是修改的关键部分：增强的Prompt ---
    if prompt == None:
        prompt_content = f"""你是一个顶级的 Neo4j 图数据库专家和 Cypher 查询生成器。你的唯一任务是根据下面提供的增强版图谱 Schema，将用户的自然语言问题转换成一个**可执行、高效、精确的 Cypher 查询语句**。

    ---
    ## Neo4j 增强图谱 Schema
    {enhanced_schema_text}
    ---

    ## 查询生成规则 (必须严格遵守):

    1.  **完全基于 Schema**: 你的查询必须严格依据上面提供的节点标签、关系类型、属性、数据样本和索引信息。**绝对不允许**虚构或猜测任何 Schema 中未列出的元素。

    2.  **利用数据样本**: Schema 中的数据样本（例如: '教授' (150)）展示了属性的真实内容和分布。请利用这些样本来正确识别问题中的实体和值。

    3.  **优先使用索引**: 这是最高效的查询方式。
        * **全文索引 (Full-text)**: 如果问题涉及模糊的文本搜索（如“关于...的文章”，“简介包含...”），并且存在对应的全文索引，**必须**使用 `CALL db.index.fulltext.queryNodes('index_name', 'search_term') YIELD node` 的语法。索引名称已在 Schema 中提供。
        * **常规索引 (B-Tree/Range)**: 对于精确匹配（例如 `name = '李华'`）或范围查询，索引会自动被使用，你只需编写标准的 `MATCH` 和 `WHERE` 子句即可。

    4.  **精确属性匹配**: 当问题涉及具体实体时（如“李华”），使用 `{{name: '李华'}}` 或 `WHERE n.name = '李华'` 进行精确属性匹配。请参考数据样本来确定属性值的格式。

    5.  **仅返回 Cypher**: 你的回答**必须且只能**是一句 Cypher 查询语句。不要包含任何解释、注释、代码块标记（如 ```cypher）或其他任何文字。

    6.  **安全限制**: 为了防止返回过多结果，所有查询都必须在末尾使用 `LIMIT`，例如 `LIMIT 25`。

    7.  **无法回答**: 如果根据提供的 Schema，问题无法被回答（例如，所需信息不存在），或者问题与图谱无关，请**只返回**一个特定的单词：`CANNOT_ANSWER`。

    ## 示例:
    - **用户问题**: "搜索一下简介里包含'机器学习'的电影"
    - **Schema 中有**: 全文索引 `movie_plot_index` on `:Movie(plot)`
    - **理想 Cypher**: `CALL db.index.fulltext.queryNodes('movie_plot_index', '机器学习') YIELD node AS m RETURN m.title, m.plot LIMIT 25`

    - **用户问题**: "Tom Hanks 演过哪些电影?"
    - **Schema 中有**: `(:Person)-[:ACTED_IN]->(:Movie)`, Person 有 `name` 属性且有 B-Tree 索引。
    - **理想 Cypher**: `MATCH (p:Person {{name: 'Tom Hanks'}})-[:ACTED_IN]->(m:Movie) RETURN m.title LIMIT 25`

    ---
    ## 用户问题:
    "{user_question}"

    ---
    ## Cypher 查询:
    """
    else:
        prompt_content = prompt
    # print(prompt_content) # 在调试时可以取消注释
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


# --- 其他辅助函数 (基本保持不变) ---
def execute_cypher_query(driver, query):
    """ 在Neo4j中执行Cypher查询并返回结果。 """
    if not driver:
        return [], "Neo4j 驱动未初始化。"
    try:
        with driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result], None
    except Exception as e:
        # 提供更详细的Cypher错误信息
        error_message = f"执行Cypher查询失败: {e}\n查询语句: {query}"
        return [], error_message

def format_results_for_display(records):
    """ 将Neo4j查询结果格式化为可读的字符串。 """
    if not records:
        return "未从知识图谱中找到相关信息。"
    return json.dumps(records, indent=2, ensure_ascii=False)

def ask_question(user_questions, enhanced_schema, schema_error, gemini_model, neo4j_driver):
    """ 主流程函数，使用增强后的Schema """
    if schema_error:
        print(f"\n无法继续，因为: {schema_error}")
        if neo4j_driver:
            neo4j_driver.close()
        return

    print("\n--- 增强版图谱 Schema (提供给LLM的上下文) ---")
    print(enhanced_schema)
    
    print("\n--- 开始进行图谱检索 ---")
    for question in user_questions:
        print(f"\n{'='*20}\n用户问题: {question}")
        
        # 3. 生成Cypher查询 (使用增强的Schema和Prompt)
        print("-> 正在生成Cypher查询...")
        cypher_query, query_gen_error = generate_cypher_query(question, enhanced_schema, gemini_model)
        
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
            if len(retrieved_records) == 0:
                print('检索到0条记录， 尝试模糊查询')
                prompt = f'''
                你是一个 Cypher 查询修复专家。下面这个用户问题，通过第一次尝试生成了查询A，但它没有返回任何结果，同时你也是一个顶级的 Neo4j 图数据库专家和 Cypher 查询生成器。你的唯一任务是根据下面提供的增强版图谱 Schema，将用户的自然语言问题转换成一个**可执行、高效、精确的 Cypher 查询语句**。
                请分析失败的可能原因（例如，严格匹配失败，概念与实际标签不符等），并生成一个**新的、更具探索性的 Cypher 查询**。

                **修复指南**:
                1.  优先考虑使用 Schema 中提供的**全文索引**进行模糊搜索。
                2.  考虑将问题中的关键词（如 '招标代理机构'）直接作为**节点标签**进行匹配。
                3.  放宽对关系类型的严格要求，寻找间接的路径。
                4.  生成的查询必须以 `LIMIT 25` 结尾。
                ## 查询生成规则 (必须严格遵守):

                1.  **完全基于 Schema**: 你的查询必须严格依据上面提供的节点标签、关系类型、属性、数据样本和索引信息。**绝对不允许**虚构或猜测任何 Schema 中未列出的元素。

                2.  **利用数据样本**: Schema 中的数据样本（例如: '教授' (150)）展示了属性的真实内容和分布。请利用这些样本来正确识别问题中的实体和值。

                3.  **优先使用索引**: 这是最高效的查询方式。
                    * **全文索引 (Full-text)**: 如果问题涉及模糊的文本搜索（如“关于...的文章”，“简介包含...”），并且存在对应的全文索引，**必须**使用 `CALL db.index.fulltext.queryNodes('index_name', 'search_term') YIELD node` 的语法。索引名称已在 Schema 中提供。
                    * **常规索引 (B-Tree/Range)**: 对于精确匹配（例如 `name = '李华'`）或范围查询，索引会自动被使用，你只需编写标准的 `MATCH` 和 `WHERE` 子句即可。

                4.  **精确属性匹配**: 当问题涉及具体实体时（如“李华”），使用 `{{name: '李华'}}` 或 `WHERE n.name = '李华'` 进行精确属性匹配。请参考数据样本来确定属性值的格式。

                5.  **仅返回 Cypher**: 你的回答**必须且只能**是一句 Cypher 查询语句。不要包含任何解释、注释、代码块标记（如 ```cypher）或其他任何文字。

                6.  **安全限制**: 为了防止返回过多结果，所有查询都必须在末尾使用 `LIMIT`，例如 `LIMIT 25`。

                7.  **无法回答**: 如果根据提供的 Schema，问题无法被回答（例如，所需信息不存在），或者问题与图谱无关，请**只返回**一个特定的单词：`CANNOT_ANSWER`。

                ---
                ## Neo4j 增强图谱 Schema
                {enhanced_schema} // 必须包含全文索引信息
                ---
                ## 原始用户问题:
                "{question}"
                ---
                ## 失败的查询 A:
                {cypher_query}
                ---
                ## 新的、更优的 Cypher 查询: '''
                # print(prompt)
                cypher_query, query_gen_error = generate_cypher_query(question, enhanced_schema, gemini_model, prompt=prompt)
                print('模糊查询 ' + cypher_query)
                retrieved_records, exec_error = execute_cypher_query(neo4j_driver, cypher_query)
                print('模糊查询检索到{}条记录'.format(len(retrieved_records)))
            formatted_context = format_results_for_display(retrieved_records)
            
            print(f"   [结果]:\n{formatted_context}")
            
    if neo4j_driver:
        neo4j_driver.close()
    print("\n完成所有查询。")
