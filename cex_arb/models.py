from sqlalchemy import Column, Index, Integer, Text, REAL, create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker


DATABASE_URL = "sqlite:///db.sql"


class Base(DeclarativeBase):
    pass


class OrderBook(Base):
    __tablename__ = "order_book"

    id = Column(Integer, primary_key=True)
    timestamp = Column(Integer, nullable=False)   # ms
    exchange = Column(Text, nullable=False)
    ticker = Column(Text, nullable=False)
    bid_price = Column(REAL, nullable=False)
    bid_quantity = Column(REAL, nullable=False)
    ask_price = Column(REAL, nullable=False)
    ask_quantity = Column(REAL, nullable=False)

    __table_args__ = (
        Index("ix_orderbook_ticker_ts", "ticker", "timestamp"),
    )

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    connect_args={"timeout": 30},
)

@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn: object, _: object) -> None:
    """Enable WAL mode so readers don't block the writer (concurrent collection + backtest)."""
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA busy_timeout=30000;")
    cur.close()

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def init_db() -> None:
    """Create all tables if they do not already exist."""
    Base.metadata.create_all(engine)