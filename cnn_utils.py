"""Utility functions for artifact_cnn.py.

Written by Robert Morgan and Adam Shandonay.
"""

import numpy as np
import torch
from torch.utils.data import Dataset


class ArtifactDataset(Dataset):
    """PyTorch Dataset for stamps."""

    def __init__(self, images, transform=None):
        self.images = images
        self.transform = transform

    def __len__(self):
        return len(self.images)

    def __getitem__(self, idx):
        if torch.is_tensor(idx):
            idx = idx.tolist()

        sample = {'image': self.images[idx]}

        if self.transform:
            sample = self.transform(sample)

        return sample

    
class ToTensor(object):
    """Convert ndarrays in sample to Tensors."""

    def __init__(self):
        self.call_kwargs = {'axis': (-1, -2), 'keepdims': True}

    def __call__(self, sample):
        image = sample['image']
        scaled_array = ((image - np.mean(image, **self.call_kwargs)) /
                        (10 * np.std(image, **self.call_kwargs)) + 0.5)
        return {'image': torch.from_numpy(scaled_array).float()}

    
class CNN(torch.nn.Module):

    def __init__(self):
        super(CNN, self).__init__()
        
        self.conv1 = torch.nn.Conv2d(
            in_channels=3, out_channels=32, kernel_size=4, stride=1, padding=2)
        self.conv2 = torch.nn.Conv2d(
            in_channels=32, out_channels=64, kernel_size=2, stride=1,
            padding=2)
        self.conv3 = torch.nn.Conv2d(
            in_channels=64, out_channels=128, kernel_size=2, stride=1,
            padding=2)
        self.dropout1 = torch.nn.Dropout2d(0.25)
        self.dropout2 = torch.nn.Dropout2d(0.5)
        self.fc1 = torch.nn.Linear(in_features=107648, out_features=328)
        self.fc2 = torch.nn.Linear(in_features=328, out_features=18)
        self.fc3 = torch.nn.Linear(in_features=18, out_features=2)
        
    def forward(self, input_tensor):
        x = self.conv1(input_tensor)
        x = torch.nn.functional.relu(x)
        x = self.conv2(x)
        x = torch.nn.functional.relu(x)
        x = self.conv3(x)
        x = torch.nn.functional.relu(x)
        x = torch.nn.functional.max_pool2d(x, kernel_size=2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = torch.nn.functional.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        x = torch.nn.functional.relu(x)
        x = self.dropout2(x)
        x = self.fc3(x)
        return torch.nn.functional.log_softmax(x, dim=1)
