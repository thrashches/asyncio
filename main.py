import asyncio
import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker


from migrations import People, engine

url = 'https://swapi.dev/api/people'
people_count = 82
engine = create_async_engine('postgresql+asyncpg://user:password@localhost/peopledb', echo=True)


async def get_people_data(resource_id: int) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{url}/{resource_id}/') as resp:
            response_json = await resp.json()
            print(response_json)
            return response_json


async def get_all_people():
    tasks = [asyncio.create_task(get_people_data(resource_id)) for resource_id in range(1, people_count + 1)]
    for task in tasks:
        result = await task
        yield result


async def process_property_list(property_type: str, property_urls: list) -> str:
    output_list = []
    for property_url in property_urls:
        property_name = await get_property_name(property_type, property_url)
        output_list.append(property_name)
    return ', '.join(output_list)


async def get_property_name(property_name: str, property_url: str) -> str:
    properties = {
        'films': 'title',
        'starships': 'name',
        'vehicles': 'name',
        'species': 'name',
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(property_url) as resp:
            response_json = await resp.json()
            print(response_json)
            return response_json[properties[property_name]]


async def insert_people_data(data: dict):
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with async_session() as session:
        async with session.begin():
            session.add(People(**data))
            await session.commit()


async def prepare_data_for_commit(response_json: dict) -> dict:
    data = {
        'birth_year': response_json.get('birth_year') or None,
        'eye_color': response_json.get('eye_color') or None,
        'gender': response_json.get('gender') or None,
        'hair_color': response_json.get('hair_color') or None,
        'height': response_json.get('height') or None,
        'homeworld': response_json.get('homeworld') or None,
        'mass': response_json.get('mass') or None,
        'name': response_json.get('name') or None,
        'skin_color': response_json.get('skin_color') or None,
    }
    if response_json.get('films'):
        data['films'] = await process_property_list('films', response_json['films'])
    if response_json.get('films'):
        data['starships'] = await process_property_list('starships', response_json['starships'])
    if response_json.get('vehicles'):
        data['vehicles'] = await process_property_list('vehicles', response_json['vehicles'])
    if response_json.get('films'):
        data['species'] = await process_property_list('species', response_json['species'])
    print(data)
    return data


async def main():
    tasks = []
    async for people in get_all_people():
        tasks.append(asyncio.create_task(
            insert_people_data(await prepare_data_for_commit(people))
        ))
    await asyncio.gather(*tasks)

asyncio.run(main())
