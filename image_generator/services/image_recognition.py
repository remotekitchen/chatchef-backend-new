from io import BytesIO

import PIL
import requests
from PIL import Image
import numpy as np
from keras.applications import MobileNetV2, VGG19, ResNet50, InceptionV3, Xception
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances

from core.utils import get_logger

logger = get_logger()


class ImageRecognition:

    # def __init__(self, model=MobileNetV2):
    #     self.ai_model = model(weights='imagenet')

    def read_image_from_url(self, url):
        print(url)
        response = requests.get(url)
        image_data = response.content
        try:
            image = Image.open(BytesIO(image_data))
        except Exception as e:
            logger.debug(f'could not identify image for: {url}')
            print('could not identify image for', url)
            raise e
        return image

    def load_images(self, links):
        images = []
        new_links = []
        for link in links:
            try:
                img = self.load_image(link)
            except Exception as e:
                continue
            images.append(img)
            new_links.append(link)
        return np.array(images), new_links

    def load_image(self, image_path):
        img = self.read_image_from_url(image_path).resize((224, 224))
        img = np.array(img) / 225.0  # Normalize pixel values between 0 and 1
        return img

    def extract_features(self, images, ai_model=MobileNetV2):
        base_model = ai_model(weights='imagenet')
        # model = Model(inputs=base_model.input, outputs=base_model.get_layer('predictions').output)
        image = np.expand_dims(images, axis=0)
        features = base_model.predict(images)
        return features

    def compare_image_to_dataset(self, image_features, dataset_features):
        similarities = cosine_similarity(image_features, dataset_features)
        # similarities = cosine_similarity(image_features.reshape(1, -1), dataset_features)
        return similarities

    def get_image_distance(self, dataset_features):
        distances = euclidean_distances(dataset_features)
        return distances

    def find_similar_images(self, similarities, threshold):
        similar_images = []
        # print(similarities)
        for i, similarity in enumerate(similarities[0]):
            print(i, "{:.7f}".format(similarity))
            if similarity > threshold:
                similar_images.append(i)
        return similar_images

    def find_similar_distances(self, distances, threshold):
        duplicate_images = []
        num_images = distances.shape[0]
        for i in range(num_images):
            for j in range(i + 1, num_images):
                similarity = distances[i, j]
                if similarity < threshold:
                    duplicate_images.append((i, j))
        return duplicate_images

    def get_similar_images(self, dataset_links, specific_image_path):
        try:
            specific_image = self.load_image(specific_image_path)
        except:
            return {}

        # Load and preprocess the dataset images
        dataset_images, new_links = self.load_images(dataset_links)

        # Extract features from the specific image
        specific_image_features = self.extract_features(np.array([specific_image]))

        # Extract features from the dataset images
        dataset_features = self.extract_features(dataset_images)

        # Compare the specific image to the dataset
        similarities = self.compare_image_to_dataset(specific_image_features, dataset_features)

        # Set the similarity threshold
        threshold = 0.9

        # Find similar images
        similar_image_indices = self.find_similar_images(similarities, threshold)

        # Print the similar images
        print("Similar images:")
        similar_images = {}
        for index in similar_image_indices:
            print(new_links[index])
            similar_images[new_links[index]] = similarities[0][index]
        return similar_images

    def get_similar_pairs(self, images):
        dataset_images, new_links = self.load_images(images)
        dataset_features = self.extract_features(dataset_images)
        distances = self.get_image_distance(dataset_features)
        print(distances)
        threshold = 0.02
        similar_image_indices = self.find_similar_distances(distances, threshold)

        similar_pairs = []
        print("Similar image pairs:")
        for pair in similar_image_indices:
            index1, index2 = pair
            print(new_links[index1], new_links[index2], distances[index1][index2])
            similar_pairs.append({
                'images': [new_links[index1], new_links[index2]],
                'match': (1 - distances[index1][index2]) * 100
            })
        return similar_pairs
