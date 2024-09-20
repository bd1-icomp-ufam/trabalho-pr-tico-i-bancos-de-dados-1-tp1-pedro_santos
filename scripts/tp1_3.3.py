# Banco de Dados I - Trabalho Prático 1
# Pedro Henrique Souza Santos
# Matrícula: 22060035

import psycopg2

# Conexão com o banco de dados
def connect_to_database():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        print("Banco de dados conectado")
        return conn
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Letra A
def get_top_comments(conn, asin):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT ClienteId, Rating, Helpful FROM AmazonProducts.Feedback
                WHERE ASIN = %s AND Rating > 3
                ORDER BY Rating DESC, Helpful DESC
                LIMIT 5;
            """, (asin,))
            top_comments = cursor.fetchall()
            return top_comments
    except psycopg2.Error as e:
        print(f"Erro ao obter os melhores comentários: {e}")
        return []

# Letra A
def get_bottom_comments(conn, asin):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT ClienteId, Rating, Helpful FROM AmazonProducts.Feedback
                WHERE ASIN = %s AND Rating <= 3
                ORDER BY Helpful DESC, Rating ASC
                LIMIT 5;
            """, (asin,))
            bottom_comments = cursor.fetchall()
            return bottom_comments
    except psycopg2.Error as e:
        print(f"Erro ao obter os piores comentários: {e}")
        return []

# Letra B
def get_similar_products(conn, asin):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT P1.Titulo, CZ.Asin_similar, P1.Salesrank FROM AmazonProducts.Produto AS P1,(SELECT P2.asin_similar FROM amazonproducts.produto AS P1 JOIN amazonproducts.produtosimilar AS P2 ON P1.asin = P2.asin where P1.asin = %s)CZ
                WHERE P1.asin = CZ.asin_similar
                ORDER BY P1.salesrank DESC;
            """, (asin,))
            similar_products = cursor.fetchall()
            return similar_products
    except psycopg2.Error as e:
        print(f"Erro ao obter os produtos similares: {e}")
        return []

# Letra C
def get_comments_evolution(conn, asin):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT "Data", ROUND(AVG(Rating) OVER (ORDER BY "Data" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS Media
                FROM AmazonProducts.Feedback AS f
                WHERE ASIN = %s
                ORDER BY f."Data";
            """, (asin,))
            comments_evolution = cursor.fetchall()
            return comments_evolution
    except psycopg2.Error as e:
        print(f"Erro ao obter a evolução dos comentários: {e}")
        return []

# Letra D
def get_best_categories(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                    SELECT 
                    P."group", 
                    P.salesrank, 
                    P.titulo
                FROM (
                    SELECT 
                        titulo, 
                        salesrank, 
                        "group",
                        ROW_NUMBER() OVER (PARTITION BY "group" ORDER BY salesrank) AS rank
                    FROM AmazonProducts.Produto
                    WHERE salesrank != -1
                ) AS P
                WHERE P.rank <= 10
                ORDER BY P."group", P.salesrank ASC;


            """)
            best_categories = cursor.fetchall()
            return best_categories
    except psycopg2.Error as e:
        print(f"Erro ao obter os melhores por categoria: {e}")
        return []

# Letra E
def get_helpfulcomments_perProducts(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT ROUND(AVG(Feedback.Helpful)::numeric, 2) AS Media_Avaliacoes_Uteis, Produto.ASIN, Produto.Titulo
                            FROM AmazonProducts.Produto AS Produto
                            JOIN AmazonProducts.Feedback AS Feedback ON Produto.ASIN = Feedback.ASIN
                            GROUP BY Produto.ASIN, Produto.Titulo
                            ORDER BY Media_Avaliacoes_Uteis DESC
                            LIMIT 10;
                           
            """)
            helps = cursor.fetchall()
            return helps
    except psycopg2.Error as e:
        print(f"Erro ao obter os produtos com as maiores media por produto: {e}")
        return []

# Letra F
def avaragecomments(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                        SELECT Produto.ASIN, MIN(Categoria.Nome) AS Nome_Categoria, ROUND(AVG(Feedback.Helpful)::numeric, 2) AS Media_Avaliacoes_Uteis
                        FROM AmazonProducts.Produto AS Produto
                        JOIN AmazonProducts.Categoria AS Categoria ON Produto.ASIN = Categoria.ASIN
                        JOIN AmazonProducts.Feedback AS Feedback ON Produto.ASIN = Feedback.ASIN
                        GROUP BY Produto.ASIN
                        ORDER BY Media_Avaliacoes_Uteis DESC
                        LIMIT 5;            
            """)
            helps = cursor.fetchall()
            return helps
    except psycopg2.Error as e:
        print(f"Erro ao obter os produtos com as maiores media por produto: {e}")
        return []

# Letra E
def top10_comments_pergroup(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                        SELECT 
                        ClienteId, 
                        "group" AS Grupo, 
                        Total_Comentarios
                    FROM (
                        SELECT 
                            F.ClienteId, 
                            P."group", 
                            COUNT(*) AS Total_Comentarios,
                            ROW_NUMBER() OVER (PARTITION BY P."group" ORDER BY COUNT(*) DESC) AS rank
                        FROM AmazonProducts.Feedback AS F
                        JOIN AmazonProducts.Produto AS P ON F.ASIN = P.ASIN
                        GROUP BY F.ClienteId, P."group"
                    ) AS ranked
                    WHERE rank <= 10;
  
            """)
            topcm = cursor.fetchall()
            return topcm
    except psycopg2.Error as e:
        print(f"Erro: {e}")
        return []
    
def main():
    conn = connect_to_database()
    if conn:
        try:
            # Produto escolhido
            product_asin = 'B00000G1IL'
            # Chamada das funções que realizam consultas no banco de dados
            top_comments = get_top_comments(conn, product_asin)
            bottom_comments = get_bottom_comments(conn, product_asin)
            similar_product = get_similar_products(conn, product_asin)
            comments_evolution = get_comments_evolution(conn, product_asin)
            best_categories = get_best_categories(conn)
            helpful_comments = get_helpfulcomments_perProducts(conn)
            avgComm = avaragecomments(conn)
            topCmm = top10_comments_pergroup(conn)

            # Processo de escrita no arquivo de saída txt
            with open('dashboard_tp1_3.3.txt', 'w') as file:
                print("Banco de Dados I - Trabalho Prático 1\nPedro Henrique Souza Santos\nMatrícula: 22060035\n", file=file)
                print(f"Produto escolhido: {product_asin}\n", file=file)

                print("a) Top 5 Comentários Mais Úteis e com Maior Avaliação:", file=file)
                for comment in top_comments:
                    print(f"Cliente ID: {comment[0]}, Avaliação: {comment[1]}, Helpful: {comment[2]}", file=file)

                print("\na) Top 5 Comentários Mais Úteis e com Menor Avaliação:", file=file)
                for comment in bottom_comments:
                    print(f"Cliente ID: {comment[0]}, Avaliação: {comment[1]}, Helpful: {comment[2]}", file=file)

                print("\nb) Produtos similares com maiores vendas do que o produto escolhido:", file=file)
                for prod in similar_product:
                    print(f"Titulo: {prod[0]}, Asin: {prod[1]}, Salesrank: {prod[2]}", file=file)

                print("\nc) Evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada:", file=file)
                for comm in comments_evolution:
                    print(f"Data:{comm[0]}, media: {comm[1]}", file=file)

                print("\nd) 10 produtos líderes de venda em cada grupo de produtos:", file=file)
                for BC in best_categories:
                    print(f"Grupo: {BC[0]} |Salesrank: {BC[1]} |Titulo: {BC[2]}", file=file)

                print("\ne) 10 produtos com a maior média de avaliações úteis positivas por produto:", file=file)
                for HP in helpful_comments:
                    print(f"Media de avaliacao: {HP[0]} |Salesrank: {HP[1]} |Grupo: {HP[2]}", file=file)

                print("\nf) 5 categorias de produto com a maior média de avaliações úteis positivas por produto:", file=file)
                for tey in avgComm:
                    print(f"Media de avaliacao: {tey[0]} |Salesrank: {tey[1]} |Grupo: {tey[2]}", file=file)

                print("\ng) 10 clientes que mais fizeram comentários por grupo de produto:", file=file)
                for mn in topCmm:
                    print(f"ID cliente: {mn[0]} |Grupo: {mn[1]} |Total de Comentarios: {mn[2]}", file=file)

                file.close()

        finally:
            conn.close()
            print(f"\nArquivo {file.name} com as consultas criado e preenchido com sucesso")

if __name__ == "__main__":
    main()