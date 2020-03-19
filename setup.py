from setuptools import setup

setup(
    name='spark_decorators',
    version='0.1.0',    
    description='Decorators for easy pyspark data pipelines',
    url='https://github.com/vintonet/spark_decorators',
    author='Luke Vinton',
    author_email='luvinton@microsoft.com',
    license='BSD 2-clause',
    packages=['spark_decorators'],
    install_requires=['pyspark>=2.4.0',              
                      ],

    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',  
        'Operating System :: POSIX :: Linux',        
        'Programming Language :: Python :: 3.5'
    ],
)