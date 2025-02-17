"""Create a baseline migrations

Revision ID: 2cfad3354442
Revises: 
Create Date: 2025-01-18 12:08:05.280833

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2cfad3354442'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('empresa',
    sa.Column('id_empresa', sa.Integer(), nullable=False),
    sa.Column('nome_empresa', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id_empresa'),
    sa.UniqueConstraint('nome_empresa')
    )
    op.create_table('relatorio',
    sa.Column('id_relatorio', sa.Integer(), nullable=False),
    sa.Column('id_empresa', sa.Integer(), nullable=True),
    sa.Column('data_inicio', sa.DateTime(), nullable=False),
    sa.Column('data_fim', sa.DateTime(), nullable=False),
    sa.Column('tipo_relatorio', sa.String(), nullable=True),
    sa.Column('ultima_atualizacao', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['id_empresa'], ['empresa.id_empresa'], ),
    sa.PrimaryKeyConstraint('id_relatorio')
    )
    op.create_table('dados_relatorio',
    sa.Column('id_dado', sa.Integer(), nullable=False),
    sa.Column('id_relatorio', sa.Integer(), nullable=True),
    sa.Column('descricao', sa.String(), nullable=True),
    sa.Column('valor', sa.Float(), nullable=True),
    sa.ForeignKeyConstraint(['id_relatorio'], ['relatorio.id_relatorio'], ),
    sa.PrimaryKeyConstraint('id_dado')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('dados_relatorio')
    op.drop_table('relatorio')
    op.drop_table('empresa')
    # ### end Alembic commands ###
