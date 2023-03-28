import asyncio
import time
from datetime import datetime
import aiohttp
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text


PG_DSN = f'postgresql+asyncpg://appadmin:1234@127.0.0.1:5431/appdb'
engine = create_async_engine(PG_DSN)
Base = declarative_base(bind=engine)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
#
class PersonModel(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(Text)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(Text)
    starships = Column(Text)
    vehicles = Column(Text)
#
async def async_generator():
    for i in range(1, 100):
        # Приостанавливаем выполнение генератора на каждой итерации
        # await asyncio.sleep(0.1)
        yield i
#
async def get_next_man(item: int):
    adres = f'https://swapi.dev/api/people/{item}/'
    return adres
#
async def itera(item:str, client_session):
    response = await client_session.get(item)
    if response.status == 404:
        return {'status': 404}
    big_jsn = await response.json()
    return big_jsn
#
async def deep_jsn_pars(big_jsn: dict, client_session, item_to_pars:str):
    homeworld_link = big_jsn.get(f'{item_to_pars}')
    response = await client_session.get(homeworld_link)
    hw = await response.json()
    homeworld = hw.get("name")
    return str(homeworld)
#
async def deep_jsn_pars2(big_jsn: dict, client_session):
    films_link = big_jsn.get('films')
    films_list = []
    for film_any in films_link:
        response2 = await client_session.get(film_any)
        xtx = await response2.json()
        xtx2 = xtx.get("title")
        films_list.append(xtx2)
    return str(films_list)
#
async def deep_jsn_pars3(big_jsn: dict, client_session, item_to_pars:str):
    species_str = big_jsn.get(f'{item_to_pars}')
    species = []
    for cpe in species_str:
        response = await client_session.get(cpe)
        cpecie = await response.json()
        species1 = cpecie.get('name')
        species.append(species1)
    if len(species) == 1:
        return species[0]
    return str(species)

#
async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    client_session = aiohttp.ClientSession()
    async for item in async_generator():
        link = await get_next_man(item)
        big_json = await itera(link, client_session)
        if big_json.get('status') == 404:
            break
        person_json = big_json
        hw1 = await deep_jsn_pars(big_json, client_session, "homeworld")
        films = await deep_jsn_pars2(big_json, client_session)
        spec = await deep_jsn_pars3(big_json, client_session, 'species')
        starships = await deep_jsn_pars3(big_json, client_session, 'starships')
        vehicles = await deep_jsn_pars3(big_json, client_session, 'vehicles')
        newperson = PersonModel(
            birth_year=person_json['birth_year'],
            eye_color=person_json['eye_color'],
            gender=person_json['gender'],
            hair_color=person_json['hair_color'],
            height=person_json['height'],
            mass=person_json['mass'],
            name=person_json['name'],
            skin_color=person_json['skin_color'],
            homeworld=hw1,
            films=films,
            species=spec,
            starships=starships,
            vehicles=vehicles,
        )
    async with Session() as session:
        session.add(newperson)
        await session.commit()
    #
    all_task = asyncio.all_tasks()
    all_task = all_task - {asyncio.current_task()}
    await asyncio.gather(*all_task)
    await client_session.close()
#
if __name__ == '__main__':
    start = datetime.now()
    asyncio.run(main())
    print(datetime.now() - start)

