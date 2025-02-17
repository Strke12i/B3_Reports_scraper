from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, create_engine
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class Empresa(Base):
    __tablename__ = 'empresa'
    id_empresa = Column(Integer, primary_key=True)
    nome_empresa = Column(String, unique=True)

class Relatorio(Base):
    __tablename__ = 'relatorio'
    id_relatorio = Column(Integer, primary_key=True)
    id_empresa = Column(Integer, ForeignKey('empresa.id_empresa'))
    data_inicio = Column(DateTime, nullable=False)
    data_fim = Column(DateTime, nullable=False)
    tipo_relatorio = Column(String)
    ultima_atualizacao = Column(DateTime, default=datetime.now)

    empresa = relationship('Empresa')

class DadosRelatorio(Base):
    __tablename__ = 'dados_relatorio'
    id_dado = Column(Integer, primary_key=True)
    id_relatorio = Column(Integer, ForeignKey('relatorio.id_relatorio'))
    descricao = Column(String)
    valor = Column(Float)

    relatorio = relationship('Relatorio')
