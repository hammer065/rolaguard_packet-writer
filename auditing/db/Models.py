from sqlalchemy import Column, DateTime, String, Integer, BigInteger, SmallInteger, Float, Boolean, ForeignKey, func, func, LargeBinary
from auditing.db import session, Base, engine
from sqlalchemy.dialects import postgresql, sqlite

BigIntegerType = BigInteger()
BigIntegerType = BigIntegerType.with_variant(postgresql.BIGINT(), 'postgresql')
BigIntegerType = BigIntegerType.with_variant(sqlite.INTEGER(), 'sqlite')


class CollectorMessage(Base):
    __tablename__ = 'collector_message'
    id = Column(BigIntegerType, primary_key=True, autoincrement=True)
    data_collector_id = Column(BigIntegerType, ForeignKey("data_collector.id"), nullable=False)
    packet_id = Column(BigIntegerType, ForeignKey("packet.id"), nullable=True)
    message = Column(String(4096), nullable=True)
    topic = Column(String(512), nullable=True)

    def save(self):
        session.add(self)
        session.flush()

    
class DataCollector(Base):
    __tablename__ = "data_collector"
    id = Column(BigIntegerType, primary_key=True, autoincrement=True)
    data_collector_type_id = Column(BigIntegerType, ForeignKey("data_collector_type.id"), nullable=False)
    name = Column(String(120), nullable=False)
    organization_id = Column(BigIntegerType, ForeignKey("organization.id"), nullable=True)
    ip = Column(String(120), nullable=False)
    port = Column(String(120), nullable=False)
    user = Column(String(120), nullable=True)
    password = Column(LargeBinary, nullable=True)

    @classmethod
    def find_one_by_ip_port_and_dctype_id(cls, dctype_id, ip, port):
        return session.query(cls).filter(cls.ip == ip).filter(cls.data_collector_type_id == dctype_id).filter(cls.port == port).first()
    
    @classmethod
    def find_one(cls, id=None):
        query = session.query(cls)
        if id:
            query = query.filter(cls.id == id)
        return query.first()

    @classmethod
    def count(cls):
        return session.query(func.count(cls.id)).scalar()

    def save(self):
        session.add(self)
        session.flush()
        commit()


class DataCollectorType(Base):
    __tablename__ = "data_collector_type"
    id = Column(BigIntegerType, primary_key=True, autoincrement=True)
    type = Column(String(30), nullable=False, unique=True)
    name = Column(String(50), nullable=False)
    
    @classmethod
    def find_one_by_type(cls, type):
        return session.query(cls).filter(cls.type == type).first()

    @classmethod
    def find_type_by_id(cls, id):
        return session.query(cls).filter(cls.id == id).first().type

    def save(self):
        session.add(self)
        session.flush()



class Packet(Base):
    __tablename__ = 'packet'
    id = Column(BigIntegerType, primary_key=True)
    date = Column(DateTime(timezone=True), nullable=False)
    topic = Column(String(256), nullable=True)
    data_collector_id = Column(BigIntegerType, ForeignKey("data_collector.id"), nullable=False)
    organization_id = Column(BigIntegerType, ForeignKey("organization.id"), nullable=False)
    gateway = Column(String(16), nullable=True)
    tmst = Column(BigIntegerType, nullable=True)
    chan = Column(SmallInteger, nullable=True)
    rfch = Column(Integer, nullable=True)
    seqn = Column(Integer, nullable=True)
    opts = Column(String(20), nullable=True)
    port = Column(Integer, nullable=True)
    freq = Column(Float, nullable=True)
    stat = Column(SmallInteger, nullable=True)
    modu = Column(String(4), nullable=True)
    datr = Column(String(50), nullable=True)
    codr = Column(String(10), nullable=True)
    lsnr = Column(Float, nullable=True)
    rssi = Column(Integer, nullable=True)
    size = Column(Integer, nullable=True)
    data = Column(String(300), nullable=True)
    m_type = Column(String(20), nullable=True)
    major = Column(String(10), nullable=True)
    mic = Column(String(8), nullable=True)
    join_eui = Column(String(16), nullable=True)
    dev_eui = Column(String(16), nullable=True)
    dev_nonce = Column(Integer, nullable=True)
    dev_addr = Column(String(8), nullable=True)
    adr = Column(Boolean, nullable=True)
    ack = Column(Boolean, nullable=True)
    adr_ack_req = Column(Boolean, nullable=True)
    f_pending = Column(Boolean, nullable=True)
    class_b = Column(Boolean, nullable=True)
    f_count = Column(Integer, nullable=True)
    f_opts = Column(String(2048), nullable=True)
    f_port = Column(Integer, nullable=True)
    error = Column(String(300), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    altitude = Column(Float, nullable=True)
    app_name = Column(String(100), nullable=True)
    dev_name = Column(String(100), nullable=True)
    gw_name= Column(String(128), nullable=True)



class Organization(Base):
    __tablename__ = "organization"
    id = Column(BigIntegerType, primary_key=True)
    name = Column(String(120), unique=True)

    @classmethod
    def find_one(cls, id=None):
        query = session.query(cls)
        if id:
            query = query.filter(cls.id == id)
        return query.first()

    @classmethod
    def count(cls):
        return session.query(func.count(cls.id)).scalar()

    def save(self):
        session.add(self)
        session.flush()
        commit()

def commit():
    session.commit()

def begin():
    session.begin()

def rollback():
    session.rollback()

 
Base.metadata.create_all(engine)