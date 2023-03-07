import json
import pandas as pd
def get_all_keys(data):
    for key, value in data.items():
        yield key
        if isinstance(value, dict):
            for subkey in get_all_keys(value):
                yield subkey


def check_keys(kafka_temp, Kafka_message):
    master_keys = set(get_all_keys(kafka_temp))
    bridge_keys = set(get_all_keys(Kafka_message))

    missing_keys = bridge_keys - master_keys
    for key in missing_keys:
        is_key_present(Kafka_message, key)
    return missing_keys


def is_key_present(dictionary, key, found=False, path=""):
    if key in dictionary:
        if found:
            print(f"'{key}' is present in Bridge Table at path '{path}'")
        else:
            print(f"'{key}' is present in Master Table")
        return True
    else:
        for k, value in dictionary.items():
            if isinstance(value, dict):
                if is_key_present(value, key, found=True, path=f"{path}.{k}" if path else k):
                    return True
    return False


def append_missing_keys(kafka_topic, kafka_message, missing_keys):
    for key in missing_keys:
        value = get_key_value(kafka_message, key)
        path = get_key_path(kafka_message, key)
        if path:
            nested_dict = get_nested_dict(kafka_topic, path)
            nested_dict[key] = value
            print(nested_dict, "nes")
        else:
            kafka_topic[key] = value
            print(kafka_topic,"topi")


def get_key_value(data, key):
    if key in data:
        return data[key]
    else:
        for k, v in data.items():
            if isinstance(v, dict):
                value = get_key_value(v, key)
                if value is not None:
                    return value
    return None


def get_key_path(data, key, path=""):
    if key in data:
        return path
    else:
        for k, v in data.items():
            if isinstance(v, dict):
                subpath = get_key_path(v, key, path=f"{path}.{k}" if path else k)
                if subpath is not None:
                    return subpath
    return None


def get_nested_dict(data, path):
    keys = path.split('.')
    nested_dict = data
    for key in keys:
        nested_dict = nested_dict.setdefault(key, {})
    return nested_dict


def main():
    with open('kafka_topic.json', 'r') as f:
        kafka_topic = json.load(f)

    with open('temp.json', 'r') as f:
        kafka_message = json.load(f)


    missing_keys = check_keys(kafka_topic, kafka_message)
    append_missing_keys(kafka_topic, kafka_message, missing_keys)
    # print(missing_keys)
    with open('kafka_topiccheck.json', 'w') as f:
        json.dump(kafka_topic, f, indent=4)

    print("Updated kafka_topic.json with missing keys")


if __name__ == '__main__':
    main()
