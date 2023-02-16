package com.jimmy.hulk.data.condition;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class ConditionContextImpl implements ConditionContext {

    private ClassLoader classLoader;

    private Environment environment;

    private ResourceLoader resourceLoader;

    private DefaultListableBeanFactory registry;

    private ConfigurableListableBeanFactory beanFactory;

    public ConditionContextImpl(DefaultListableBeanFactory registry) throws Exception {
        this.registry = registry;
        this.environment = registry.getBean(Environment.class);
        this.beanFactory = deduceBeanFactory(registry);
        this.resourceLoader = deduceResourceLoader(registry);
        this.classLoader = deduceClassLoader(resourceLoader, this.beanFactory);
    }

    private ResourceLoader deduceResourceLoader(BeanDefinitionRegistry source) {
        if (source instanceof ResourceLoader) {
            return (ResourceLoader) source;
        }
        return new DefaultResourceLoader();
    }

    private ConfigurableListableBeanFactory deduceBeanFactory(BeanDefinitionRegistry source) {
        if (source instanceof ConfigurableListableBeanFactory) {
            return (ConfigurableListableBeanFactory) source;
        }
        if (source instanceof ConfigurableApplicationContext) {
            return (((ConfigurableApplicationContext) source).getBeanFactory());
        }
        return null;
    }

    private ClassLoader deduceClassLoader(ResourceLoader resourceLoader, ConfigurableListableBeanFactory beanFactory) {
        if (resourceLoader != null) {
            ClassLoader classLoader = resourceLoader.getClassLoader();
            if (classLoader != null) {
                return classLoader;
            }
        }
        if (beanFactory != null) {
            return beanFactory.getBeanClassLoader();
        }
        return ClassUtils.getDefaultClassLoader();
    }

    @Override
    public BeanDefinitionRegistry getRegistry() {
        Assert.state(this.registry != null, "No BeanDefinitionRegistry available");
        return this.registry;
    }

    @Override
    public ConfigurableListableBeanFactory getBeanFactory() {
        return this.beanFactory;
    }

    @Override
    public Environment getEnvironment() {
        return this.environment;
    }

    @Override
    public ResourceLoader getResourceLoader() {
        return this.resourceLoader;
    }

    @Override
    public ClassLoader getClassLoader() {
        return this.classLoader;
    }
}
