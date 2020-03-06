package ink.anur.inject

import ink.anur.exception.DuplicateBeanException
import ink.anur.exception.KanashiException
import ink.anur.exception.NoSuchBeanException
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.net.JarURLConnection
import java.net.URL
import java.util.HashSet
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

    class NigateBeanContainer {

        val UNDEFINE_ALIAS = "UNDEFINE"

        /**
         * bean 名字与 bean 的映射，只能一对一
         */
        private val BEAN_NAME_MAPPING = mutableMapOf<String, Any>()

        /**
         * bean 类型与 bean 的映射，可以一对多
         */
        private val BEAN_CLASS_MAPPING = mutableMapOf<Class<*>, MutableSet<Any>>()

        /**
         * 存储 “类” 与其 “类定义” 的映射集合
         */
        private val BEAN_DEFINITION_MAPPING = mutableMapOf<String, NigateBeanDefinition>()

        /**
         * 存储接口类与其 “实现们” 的映射集合
         */
        private val INTERFACE_MAPPING = mutableMapOf<Class<*>, MutableSet<Class<*>>>()

        /**
         * 避免因为路径问题导致初始化重复扫描
         */
        private val initDuplicateCleaner = mutableSetOf<String>()

        fun autoRegister(path: String, clazz: Class<*>, anno: NigateBean, fromJar: Boolean) {
            if (initDuplicateCleaner.add(path)) {
                val name = register(clazz.newInstance(), anno.name)
                BEAN_DEFINITION_MAPPING[name] = NigateBeanDefinition(fromJar)
            } else {
                // ignore
            }
        }

        /**
         * 注册一个bean，优先取 alias 取不到则使用 bean 的 simpleName
         */
        fun register(bean: Any, alias: String? = null): String {
            val clazz = bean.javaClass
            val actualName = if (alias == null || alias == UNDEFINE_ALIAS) clazz.simpleName else alias
            val duplicate = BEAN_NAME_MAPPING.putIfAbsent(actualName, bean)
            duplicate?.also { throw DuplicateBeanException("bean $clazz 存在重复注册的情况，请使用 @NigateBean(name = alias) 为其中一个起别名") }

            BEAN_CLASS_MAPPING.compute(bean.javaClass) { _, v ->
                val set = v ?: mutableSetOf()
                set.add(bean)
                set
            }

            clazz.interfaces.forEach {
                INTERFACE_MAPPING.compute(it) { _, v ->
                    val set = v ?: mutableSetOf()
                    set.add(clazz)
                    set
                }
            }
            return actualName
        }

        private fun <T> getBeanByClassFirstThenName(clazz: Class<T>): T {
            var result: T? = null
            try {
                try {
                    result = getBeanByClass(clazz)
                } catch (t: Throwable) {
                    result = Nigate.getBeanByName(clazz.simpleName) as T
                }
            } catch (t: Throwable) {
                throw NoSuchBeanException("无法根据类 ${clazz.simpleName} 找到唯一的 Bean 或 无法根据名字 ${clazz.simpleName} 找到指定的 Bean ")
            }
            return result!!
        }

        fun getBeanByName(name: String): Any = BEAN_NAME_MAPPING[name] ?: throw NoSuchBeanException("bean named $name is not managed")

        fun <T> getBeanByClass(clazz: Class<T>): T {
            val l = BEAN_CLASS_MAPPING[clazz] ?: throw NoSuchBeanException("bean with type $clazz is not managed")
            if (l.size > 1) {
                throw DuplicateBeanException("bean $clazz 存在多实例的情况，请使用 @NigateInject(name = alias) 选择注入其中的某个 bean")
            }
            return (l.first()) as T
        }

        /**
         * 为某个bean注入成员变量
         */
        fun inject(injected: Any) {
            for (kProperty in injected::class.declaredMemberProperties) {
                for (annotation in kProperty.annotations) {
                    if (annotation.annotationClass == NigateInject::class) {
                        annotation as NigateInject
                        val javaField = kProperty.javaField!!

                        val fieldName = kProperty.name
                        val fieldClass = kProperty.returnType.javaType as Class<*>
                        var injection: Any? = null
                        if (annotation.name == UNDEFINE_ALIAS) {// 如果没有指定别名注入
                            if (fieldClass.isInterface) {// 如果是 接口类型
                                val mutableSet = INTERFACE_MAPPING[fieldClass]
                                if (mutableSet == null) {
                                    throw NoSuchBeanException("不存在接口类型为 $fieldClass 的 Bean！")
                                } else {
                                    if (mutableSet.size == 1) {// 如果只有一个实现，则注入此实现
                                        injection = getBeanByClassFirstThenName(mutableSet.first())
                                    } else if (annotation.useLocalFirst) {// 如果优先使用本地写的类
                                        val localBean = mutableSet.takeIf { BEAN_DEFINITION_MAPPING[it.javaClass.simpleName]?.fromJar == false }
                                        when {
                                            localBean == null -> throw DuplicateBeanException("bean ${injected.javaClass} " +
                                                " 将注入的属性 $fieldName 为接口类型，且存在多个来自【依赖】的子类实现，请改用 @NigateInject(name) 来指定别名注入")
                                            localBean.size > 1 -> throw DuplicateBeanException("bean ${injected.javaClass} " +
                                                " 将注入的属性 $fieldName 为接口类型，且存在多个来自【本地】的子类实现，请改用 @NigateInject(name) 来指定别名注入")
                                            else -> injection = getBeanByClassFirstThenName(mutableSet.first())
                                        }
                                    }
                                }
                            } else {// 如果不是接口类型，直接根据类来注入
                                injection = getBeanByClass(fieldClass)
                            }
                        } else {// 如果指定了别名，直接根据别名注入
                            injection = getBeanByName(annotation.name)
                        }

                        javaField.isAccessible = true
                        javaField.set(injected, injection)
                    }
                }
            }
        }
    }

    class NigateBeanDefinition(
        /**
         * fromJar 代表此实现是有默认的实现而且实现在继承的maven里就已经写好
         */
        val fromJar: Boolean
    )

    /**
     * bean 容器
     */
    private val beanContainer = NigateBeanContainer()

    private val logger = LoggerFactory.getLogger(this::class.java)

    private var OVER_REGISTER: Boolean = false

    init {
        val start = System.currentTimeMillis()
        logger.info("Nigate ==> Registering..")
        val scans = doScan()
        OVER_REGISTER = true
        logger.info("Nigate ==> Register complete")

        logger.info("Nigate ==> Injecting..")
        for (bean in scans) {
            beanContainer.inject(bean)
        }
        logger.info("Nigate ==> Inject complete")

        logger.info("Nigate ==> Invoking postConstruct..")
        for (bean in scans) {
            postConstruct(bean, true)
        }
        logger.info("Nigate ==> Invoke postConstruct complete")

        logger.info("Nigate Started in ${(System.currentTimeMillis() - start) / 1000f} seconds")
    }

    fun <T> getBeanByClass(clazz: Class<T>): T = beanContainer.getBeanByClass(clazz)

    fun getBeanByName(name: String): Any = beanContainer.getBeanByName(name)

    /**
     * 注册某个bean
     */
    fun register(bean: Any, alias: String? = null) {
        val actualName = beanContainer.register(bean, alias)
        logger.debug("bean named [$actualName] is managed by Nigate")
    }

    fun inject(injected: Any) {
        if (!OVER_REGISTER) {
            throw KanashiException("暂时不支持在初始化完成前进行构造注入！")
        }
        beanContainer.inject(injected)
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
                    val path = "$packagePath.${classPath.replace(".class", "")}"
                    val aClass = Class.forName(path)
                    for (annotation in aClass.annotations) {
                        if (annotation.annotationClass == NigateBean::class) {
                            beanContainer.autoRegister(path, aClass, annotation as NigateBean, false)
                            res.add(aClass)
                        }
                    }
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
                    val aClass = Class.forName(className)
                    for (annotation in aClass.annotations) {
                        if (annotation.annotationClass == NigateBean::class) {
                            beanContainer.autoRegister(className, aClass, annotation as NigateBean, true)
                            res.add(aClass)
                        }
                    }
                } catch (e: ClassNotFoundException) {
                    e.printStackTrace()
                }

            }
        }
        return res
    }

    private fun doScan(): MutableSet<Class<*>> {
        val classes = getClasses("ink.anur")
        return classes.filter { clazz -> clazz.annotations.let { annos -> annos.any { it.annotationClass == NigateBean::class } } }.toMutableSet()
    }
}