/**
 * Copyright (C) 2014 Dasasian (damith@dasasian.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dasasian.chok.protocol;

import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.io.Serializable;
import java.util.List;

public class BlockingQueue<T extends Serializable> {

    protected final ZkClient zkClient;
    private final String elementsPath;
    public BlockingQueue(ZkClient zkClient, String rootPath) {
        this.zkClient = zkClient;
        elementsPath = rootPath + "/operations";
        this.zkClient.createPersistent(rootPath, true);
        this.zkClient.createPersistent(elementsPath, true);
    }

    private String getElementRoughPath() {
        return getElementPath("operation" + "-");
    }

    public String getElementPath(String elementId) {
        return elementsPath + "/" + elementId;
    }

    /**
     * @param element the element to add
     * @return the id of the element in the queue
     */
    public String add(T element) {
        try {
            String sequential = zkClient.createPersistentSequential(getElementRoughPath(), element);
            return sequential.substring(sequential.lastIndexOf('/') + 1);
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
    }

    public T remove() throws InterruptedException {
        Element<T> element = getFirstElement();
        zkClient.delete(getElementPath(element.getName()));
        return element.getData();
    }

    public boolean containsElement(String elementId) {
        String zkPath = getElementPath(elementId);
        return zkClient.exists(zkPath);
    }

    public T peek() throws InterruptedException {
        Element<T> element = getFirstElement();
        if (element == null) {
            return null;
        }
        return element.getData();
    }

    public int size() {
        return zkClient.getChildren(elementsPath).size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    private String getSmallestElement(List<String> list) {
        String smallestElement = list.get(0);
        for (String element : list) {
            if (element.compareTo(smallestElement) < 0) {
                smallestElement = element;
            }
        }

        return smallestElement;
    }

    @SuppressWarnings("unchecked")
    protected Element<T> getFirstElement() throws InterruptedException {
        final Object mutex = new Object();
        IZkChildListener notifyListener = (parentPath, currentChilds) -> {
            synchronized (mutex) {
                mutex.notify();
            }
        };
        try {
            while (true) {
                List<String> elementNames;
                synchronized (mutex) {
                    elementNames = zkClient.subscribeChildChanges(elementsPath, notifyListener);
                    while (elementNames == null || elementNames.isEmpty()) {
                        mutex.wait();
                        elementNames = zkClient.getChildren(elementsPath);
                    }
                }
                String elementName = getSmallestElement(elementNames);
                try {
                    String elementPath = getElementPath(elementName);
                    return new Element<>(elementName, (T) zkClient.readData(elementPath));
                } catch (ZkNoNodeException e) {
                    // somebody else picked up the element first, so we have to
                    // retry with the new first element
                }
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        } finally {
            zkClient.unsubscribeChildChanges(elementsPath, notifyListener);
        }
    }

    protected static class Element<T> {
        private final String name;
        private final T data;

        public Element(String name, T data) {
            this.name = name;
            this.data = data;
        }

        public String getName() {
            return name;
        }

        public T getData() {
            return data;
        }
    }

}
