package ink.anur.inject

import ink.anur.exception.KanashiException
import ink.anur.exception.NoSuchBeanException
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.net.JarURLConnection
import java.net.URL
import java.util.HashSet
import java.util.function.BiFunction
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.javaField
import kotlin.reflect.jvm.javaType
import kotlin.system.exitProcess

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 扫描系列
 */
object Nigate {

    class NigateBeanDefinition(
        /**
         * fromJar 代表此实现是有默认的实现而且实现在继承的maven里就已经写好
         */
        val fromJar: Boolean
    )

    /**
     * 存储 类与其类定义 的映射集合
     */
    private val BEAN_DEFINITION_MAPPING = mutableMapOf<Class<*>, NigateBeanDefinition>()

    /**
     * 存储 接口类与其实现们 的映射集合
     */
    private val INTERFACE_MAPPING = mutableMapOf<Class<*>, MutableSet<Class<*>>>()

    /**
     * bean 的容器们
     */
    private val BEAN_MAPPING = mutableMapOf<String, Any>()

    private val logger = LoggerFactory.getLogger(this::class.java)

    private var OVER_REGISTER: Boolean = false

    init {
        val start = System.currentTimeMillis()
        val scans = doScan()

        for (scan in scans) {
            analyzeInterfaces(scan)
        }

        logger.info("Nigate ==> Registering..")
        for (clazz in scans) {
            register(clazz.newInstance(), clazz.simpleName)
        }
        OVER_REGISTER = true
        logger.info("Nigate ==> Register complete")

        logger.info("Nigate ==> Injecting..")
        for (bean in BEAN_MAPPING.values) {
            inject(bean)
        }
        logger.info("Nigate ==> Inject complete")

        logger.info("Nigate ==> Invoking postConstruct..")
        for (bean in BEAN_MAPPING.values) {
            postConstruct(bean, true)
        }
        logger.info("Nigate ==> Invoke postConstruct complete")

        logger.info("Nigate Started in ${(System.currentTimeMillis() - start) / 1000f} seconds")
    }

    fun getBeanByName(name: String): Any = BEAN_MAPPING[name] ?: throw NoSuchBeanException("bean named $name is not managed")

    fun <T> getBeanByClass(clazz: Class<T>): T = (BEAN_MAPPING[clazz.simpleName] ?: throw NoSuchBeanException("bean with type $clazz is not managed")) as T

    /**
     * 注册某个bean
     */
    fun register(bean: Any, name: String? = null) {
        val actualName = name ?: bean.javaClass.simpleName
        BEAN_MAPPING[actualName] = bean
        logger.debug("bean named [$actualName] is managed by Nigate")
    }

    fun initInject(injected: Any) {
        if (!OVER_REGISTER) {
            throw KanashiException("暂时不支持在初始化完成前进行构造注入！")
        }
        inject(injected)
    }

    /**
     * 为某个bean注入成员变量
     */
    private fun inject(injected: Any) {
        for (kProperty in injected::class.declaredMemberProperties) {
            for (annotation in kProperty.annotations) {
                if (annotation.annotationClass == NigateInject::class) {
                    val injection = BEAN_MAPPING[(kProperty.returnType.javaType as Class<*>).simpleName]
                    val javaField = kProperty.javaField!!
                    javaField.isAccessible = true
                    javaField.set(injected, injection)
                }
            }
        }
    }

    fun postConstruct(bean: Any, startUp: Boolean) {
        for (memberFunction in bean::class.memberFunctions) {
            for (annotation in memberFunction.annotations) {
                if (annotation.annotationClass == NigatePostConstruct::class) {
                    try {
                        memberFunction.isAccessible = true
                        memberFunction.call(bean)
                    } catch (e: Exception) {
                        logger.error("class [${bean::class}] invoke post construct method [${memberFunction.name}] error : ${e.message}")
                        e.printStackTrace()
                        if (startUp) {
                            exitProcess(1)
                        }
                    }
                }
            }
        }
    }

    fun getClasses(packagePath: String): Set<Class<*>> {
        val res = HashSet<Class<*>>()
        val path = packagePath.replace(".", "/")
        val resources = Thread.currentThread().contextClassLoader.getResources(path)

        while (resources.hasMoreElements()) {
            val url = resources.nextElement()
            val protocol = url.protocol
            if ("jar".equals(protocol, ignoreCase = true)) {
                try {
                    res.addAll(getJarClasses(url, packagePath))
                } catch (e: IOException) {
                    e.printStackTrace()
                    return res
                }

            } else if ("file".equals(protocol, ignoreCase = true)) {
                res.addAll(getFileClasses(url, packagePath))
            }
        }
        return res
    }

    //获取file路径下的class文件
    private fun getFileClasses(url: URL, packagePath: String): Set<Class<*>> {
        val res = HashSet<Class<*>>()
        val filePath = url.file
        val dir = File(filePath)
        val list = dir.list() ?: return res
        for (classPath in list) {
            if (classPath.endsWith(".class")) {
                try {
                    val aClass = Class.forName("$packagePath.${classPath.replace(".class", "")}")
                    res.add(aClass)
                } catch (e: ClassNotFoundException) {
                    e.printStackTrace()
                }

            } else {
                res.addAll(getClasses("$packagePath.$classPath"))
            }
        }
        return res
    }

    //使用JarURLConnection类获取路径下的所有类
    @Throws(IOException::class)
    private fun getJarClasses(url: URL, packagePath: String): Set<Class<*>> {
        val res = HashSet<Class<*>>()
        val conn = url.openConnection() as JarURLConnection
        val jarFile = conn.jarFile
        val entries = jarFile.entries()
        while (entries.hasMoreElements()) {
            val jarEntry = entries.nextElement()
            val name = jarEntry.name
            if (name.contains(".class") && name.replace("/".toRegex(), ".").startsWith(packagePath)) {
                val className = name.substring(0, name.lastIndexOf(".")).replace("/", ".")
                try {
                    val clazz = Class.forName(className)
                    res.add(clazz)
                } catch (e: ClassNotFoundException) {
                    e.printStackTrace()
                }

            }
        }
        return res
    }

    /**
     * 分析一个类的接口继承关系，方便注入接口
     */
    private fun analyzeInterfaces(clazz: Class<*>) {
        clazz.interfaces.forEach {
            INTERFACE_MAPPING.compute(clazz, BiFunction { _, v ->
                (v ?: mutableSetOf()).also { s -> s.add(it) }
            })
        }
    }

    private fun doScan(): MutableSet<Class<*>> {
        val classes = getClasses("ink.anur")
        return classes.filter { clazz -> clazz.annotations.let { annos -> annos.any { it.annotationClass == NigateBean::class } } }.toMutableSet()
    }
}