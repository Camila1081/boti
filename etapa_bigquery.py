import pandas_gbq
import pandas as pd

def etapa_bigquery (project_name):


    #TABELA 1
    sql = """
    SELECT ANO_VENDA, MES_VENDA, SUM(QTD_VENDA) as TOTAL_VENDAS
    FROM `boti-347200.dados_anos.base_consolidada` 
    GROUP BY ANO_VENDA, MES_VENDA
    ORDER BY ANO_VENDA, MES_VENDA
    """
    df_mes_venda = pandas_gbq.read_gbq(sql, project_id=project_name)
    df_mes_venda.to_gbq(destination_table = 'dados_anos.base_consolidada_mensal', project_id=project_name,if_exists='replace' )

    #TABELA 2
    sql = """
    SELECT MARCA, LINHA, SUM(QTD_VENDA) as TOTAL_VENDAS
    FROM `boti-347200.dados_anos.base_consolidada` 
    GROUP BY MARCA, LINHA
    ORDER BY MARCA, LINHA
    """
    df_marca = pandas_gbq.read_gbq(sql, project_id=project_name)
    df_marca.to_gbq(destination_table = 'dados_anos.base_consolidada_marca_linha', project_id=project_name,if_exists='replace' )

    #TABELA 3
    sql = """
    SELECT MARCA, ANO_VENDA, MES_VENDA, SUM(QTD_VENDA) as TOTAL_VENDAS
    FROM `boti-347200.dados_anos.base_consolidada` 
    GROUP BY MARCA, ANO_VENDA, MES_VENDA
    ORDER BY MARCA, ANO_VENDA, MES_VENDA
    """
    df_marca = pandas_gbq.read_gbq(sql, project_id=project_name)
    df_marca.to_gbq(destination_table = 'dados_anos.base_consolidada_marca_mensal', project_id=project_name,if_exists='replace' )

    #TABELA 4
    sql = """
    SELECT LINHA, ANO_VENDA, MES_VENDA, SUM(QTD_VENDA) as TOTAL_VENDAS
    FROM `boti-347200.dados_anos.base_consolidada` 
    GROUP BY LINHA, ANO_VENDA, MES_VENDA
    ORDER BY LINHA, ANO_VENDA, MES_VENDA
    """
    df_linha = pandas_gbq.read_gbq(sql, project_id=project_name)
    df_linha.to_gbq(destination_table = 'dados_anos.base_consolidada_linha_mensal', project_id=project_name,if_exists='replace' )