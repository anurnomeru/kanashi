package ink.anur.inject

import ink.anur.exception.NoSuchBeanException
import org.slf4j.LoggerFactory
import java.io.File
import kotlin.jvm.internal.ClassReference

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 扫描系列
 */
object Nigate {

    @JvmStatic
    fun main(args: Array<String>) {
        println()
    }

    private val defaultClassPath = this.javaClass.getResource("/").path

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val BEAN_MAPPING = mutableMapOf<String, Any>()

    init {
        doScan(File(defaultClassPath))
    }

    fun getBeanByName(name: String): Any = BEAN_MAPPING[name] ?: throw NoSuchBeanException("bean named $name is not managed")

    private fun register(bean: Any, name: String? = null) {
        val actualName = name ?: bean.javaClass.simpleName
        BEAN_MAPPING[actualName] = bean
        logger.debug("bean named [$actualName] is managed by Nigate")
    }

    private fun doScan(file: File, packageName: String = "", start: Boolean = true) {
        if (file.isDirectory) {
            for (f in file.listFiles()) {
                val pn = if (start) {
                    packageName
                } else {
                    if (packageName.isEmpty()) {
                        file.name
                    } else {
                        packageName + "." + file.name
                    }
                }
                doScan(f, pn, false)
            }
        } else {
            if (file.name.endsWith(".class")) {
                val name = if (packageName.isEmpty()) {
                    file.name.substring(0, file.name.lastIndexOf("."))
                } else {
                    packageName + "." + file.name.substring(0, file.name.lastIndexOf("."))
                }

                val clazz = Class.forName(name)
                var containBeanAnnotation = false
                for (annotation in clazz.annotations) {
                    if ((annotation.annotationClass as ClassReference).jClass == NigateBean::class.java) {
                        containBeanAnnotation = true
                        break
                    }
                }

                if (containBeanAnnotation) {
                    register(clazz.newInstance(), clazz.simpleName)
                }
            }
        }
    }
}